using System.Globalization;
using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Finds whether DDL being replicated uses the session's temporary objects other than tables, which PostgreSQL notes
/// itself only as it opens a temporary relation, creates an object in the temporary schema, or drops or locks a
/// temporary object. Such DDL stays on this node, since the other nodes' sessions don't have the objects.
/// </summary>
internal static unsafe class TemporaryObjectUse
{
    // The catalogs of relations, attribute defaults, indexes, inheritance, foreign tables and types, from
    // catalog/pg_*_d.h.
    private const uint RelationRelationId = 1259;
    private const uint IndexRelationId = 2610;
    private const uint InheritsRelationId = 2611;
    private const uint ForeignTableRelationId = 3118;
    private const uint TypeRelationId = 1247;

    // The token the lexer returns for any keyword, given it in place of the parser's own tokens.
    private const ushort KeywordToken = ushort.MaxValue;

    // The objects that the DDL being replicated creates or changes, and whether it runs or inlines one of the
    // session's temporary functions, noted while the session has temporary objects.
    private static readonly List<(uint ClassId, uint ObjectId, int SubId)> s_changedObjects = [];
    private static bool s_ranTemporaryFunction;

    /// <summary>
    /// Starts noting what DDL being replicated uses.
    /// </summary>
    internal static void Begin()
    {
        s_changedObjects.Clear();
        s_ranTemporaryFunction = false;
    }

    /// <summary>
    /// Notes an object that DDL being replicated creates or changes, or a function it runs, from the object access
    /// hook. An index's, an inheritance's and a foreign table's own catalog rows stand for their relation, and an
    /// attribute default is reported by its table and column.
    /// </summary>
    /// <param name="access">What the DDL does with the object.</param>
    /// <param name="classId">The object's catalog.</param>
    /// <param name="objectId">The object.</param>
    /// <param name="subId">The object's column, or zero.</param>
    internal static void NoteObjectAccess(ObjectAccessType access, uint classId, uint objectId, int subId)
    {
        if (!DdlReplication.Running || TemporaryNamespace() == 0)
        {
            return;
        }

        if (access == ObjectAccessType.OAT_FUNCTION_EXECUTE)
        {
            NoteFunctionUse(objectId);
        }
        else if (access is ObjectAccessType.OAT_POST_CREATE or ObjectAccessType.OAT_POST_ALTER)
        {
            s_changedObjects.Add(classId is IndexRelationId or InheritsRelationId or ForeignTableRelationId
                ? (RelationRelationId, objectId, 0)
                : (classId, objectId, subId));
        }
    }

    /// <summary>
    /// Notes a function that DDL being replicated is about to run or inline, as an expression of
    /// <c>ALTER TABLE ... USING</c> does, which leaves no dependency behind.
    /// </summary>
    /// <param name="funcid">The function.</param>
    internal static void NoteFunctionUse(uint funcid)
    {
        if (DdlReplication.Running && TemporaryNamespace() != 0 && NativeMethods.isTempNamespace(NativeMethods.get_func_namespace(funcid)))
        {
            s_ranTemporaryFunction = true;
        }
    }

    /// <summary>
    /// Checks whether DDL being replicated that has run used the session's temporary objects: opened one, names one
    /// with the temporary schema, names a temporary type without one in a cast, ran or inlined a temporary function,
    /// changed a temporary object, made an object depend on one, or commented on, labeled or granted on one. Such DDL
    /// stays on this node when it created or changed only temporary objects, and fails when it created or changed
    /// others, which the other nodes couldn't follow, and on which later DDL would then fail there. A DROP leaves out
    /// its temporary objects itself.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    /// <param name="command">Its text.</param>
    /// <param name="openedTemporary">Whether it opened a temporary relation, as PostgreSQL notes.</param>
    /// <returns>Whether it did.</returns>
    internal static bool UsedBy(Node* parsetree, string command, bool openedTemporary)
    {
        uint nspoid = TemporaryNamespace();
        if (nspoid == 0 || parsetree->type == NodeTag.T_DropStmt)
        {
            return openedTemporary && parsetree->type != NodeTag.T_DropStmt;
        }

        string schema = Worf.FromServer(NativeMethods.get_namespace_name(nspoid));
        bool used = openedTemporary || s_ranTemporaryFunction || NamesTemporaryObjects(command, schema);
        AddTargets(parsetree);
        if (s_changedObjects.Count == 0)
        {
            return used;
        }

        NativeMethods.CommandCounterIncrement();
        long[] classIds = [.. s_changedObjects.Select(changed => (long)changed.ClassId)];
        long[] objectIds = [.. s_changedObjects.Select(changed => (long)changed.ObjectId)];
        long[] subIds = [.. s_changedObjects.Select(changed => (long)changed.SubId)];

        // An object in a catalog with a schema is checked for its own schema, and every object for what it depends on.
        // An object outlasts the session unless it's in the temporary schema, or is part of a temporary relation.
        SpiResult result = Elevated.Run(() => Spi.Select("""
            WITH o AS (
                SELECT c.classid, coalesce(ad.oid, c.objid) AS objid, CASE WHEN ad.oid IS NULL THEN c.objsubid ELSE 0 END AS objsubid,
                    CASE WHEN c.classid IN ('pg_catalog.pg_class'::pg_catalog.regclass, 'pg_catalog.pg_attrdef'::pg_catalog.regclass) THEN c.objid
                        WHEN c.classid = 'pg_catalog.pg_trigger'::pg_catalog.regclass THEN (SELECT tgrelid FROM pg_catalog.pg_trigger WHERE oid = c.objid)
                        WHEN c.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN (SELECT ev_class FROM pg_catalog.pg_rewrite WHERE oid = c.objid)
                        WHEN c.classid = 'pg_catalog.pg_policy'::pg_catalog.regclass THEN (SELECT polrelid FROM pg_catalog.pg_policy WHERE oid = c.objid)
                    END AS relid
                FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.oid[]), pg_catalog.unnest($2::pg_catalog.oid[]),
                    pg_catalog.unnest($3::pg_catalog.int4[])) AS c (classid, objid, objsubid)
                LEFT JOIN pg_catalog.pg_attrdef ad
                    ON c.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass AND ad.adrelid = c.objid AND ad.adnum = c.objsubid
            )
            SELECT
                EXISTS (
                    SELECT FROM o
                    WHERE (o.classid IN ('pg_catalog.pg_class'::pg_catalog.regclass, 'pg_catalog.pg_type'::pg_catalog.regclass,
                            'pg_catalog.pg_proc'::pg_catalog.regclass, 'pg_catalog.pg_operator'::pg_catalog.regclass,
                            'pg_catalog.pg_collation'::pg_catalog.regclass, 'pg_catalog.pg_conversion'::pg_catalog.regclass,
                            'pg_catalog.pg_constraint'::pg_catalog.regclass, 'pg_catalog.pg_opclass'::pg_catalog.regclass,
                            'pg_catalog.pg_opfamily'::pg_catalog.regclass, 'pg_catalog.pg_statistic_ext'::pg_catalog.regclass,
                            'pg_catalog.pg_ts_parser'::pg_catalog.regclass, 'pg_catalog.pg_ts_dict'::pg_catalog.regclass,
                            'pg_catalog.pg_ts_template'::pg_catalog.regclass, 'pg_catalog.pg_ts_config'::pg_catalog.regclass)
                            AND (pg_catalog.pg_identify_object(o.classid, o.objid, 0)).schema = $4)
                        OR EXISTS (
                            SELECT FROM pg_catalog.pg_depend d
                            WHERE d.classid = o.classid AND d.objid = o.objid AND (o.objsubid = 0 OR d.objsubid IN (0, o.objsubid))
                                AND (pg_catalog.pg_identify_object(d.refclassid, d.refobjid, 0)).schema = $4)),
                EXISTS (
                    SELECT FROM o
                    WHERE CASE WHEN o.relid IS NOT NULL
                        THEN (SELECT relpersistence <> 't' FROM pg_catalog.pg_class WHERE oid = o.relid)
                        ELSE (SELECT i.schema IS DISTINCT FROM $4 FROM pg_catalog.pg_identify_object(o.classid, o.objid, 0) AS i
                            WHERE i.identity IS NOT NULL) END)
            """, SpiParameter.Create(classIds), SpiParameter.Create(objectIds), SpiParameter.Create(subIds), SpiParameter.Create(schema)));
        used |= result[0].Get<bool>(0);
        if (used && result[0].Get<bool>(1))
        {
            throw new PgException("0A000", "cannot create or change objects that outlast the session with its temporary objects while worf.enable_ddl_replication is on",
                hint: "The other nodes don't have the temporary objects. Use permanent ones, or write out what the statement takes from them.");
        }

        return used;
    }

    /// <summary>
    /// Checks whether an object a DROP names is one of the session's temporary objects, locking it as the DROP will.
    /// </summary>
    /// <param name="type">The object's type.</param>
    /// <param name="target">The object's name, as the DROP has it.</param>
    /// <returns>Whether it is.</returns>
    internal static bool IsTemporaryTarget(ObjectType type, Node* target)
    {
        if (TemporaryNamespace() == 0)
        {
            return false;
        }

        RelationData* relation = null;
        ObjectAddress address = NativeMethods.get_object_address(type, target, &relation, (int)PgLockMode.AccessExclusive, true);
        if (relation is not null)
        {
            bool temporary = relation->rd_rel->relpersistence == (sbyte)'t';
            NativeMethods.relation_close(relation, (int)PgLockMode.None);
            return temporary;
        }

        uint nspoid = address.objectId == 0 ? 0 : NativeMethods.get_object_namespace(&address);
        return nspoid != 0 && NativeMethods.isTempNamespace(nspoid);
    }

    /// <summary>
    /// Scans a statement with PostgreSQL's own lexer, as the parser read it, for a name qualified with the session's
    /// temporary schema, by its pg_temp alias or its own name, for a type named without a schema in a cast, written
    /// with <c>::</c>, <c>CAST</c> or <c>TREAT</c>, that is temporary, as PostgreSQL looks types up in the temporary
    /// schema first, and for a column's type taken with <c>%TYPE</c> from a temporary table. Names of tables and of
    /// the types of what the statement stores are found otherwise.
    /// </summary>
    private static bool NamesTemporaryObjects(string command, string schema)
    {
        List<(int Token, string? Text)> samples = Lex("x U&\"x\" 'x' ::", []);
        int identifier = samples[0].Token;
        int unicodeIdentifier = samples[1].Token;
        int stringConstant = samples[2].Token;
        int typecast = samples[3].Token;
        List<(int Token, string? Text)> tokens = Names(Lex(command, [identifier, unicodeIdentifier, stringConstant]), identifier, unicodeIdentifier,
            stringConstant);
        bool IsName(int i) => i >= 0 && i < tokens.Count && tokens[i].Text is not null;
        bool Qualifies(int i) => i >= 0 && i < tokens.Count && tokens[i].Token == '.';

        Stack<int> casts = [];
        int depth = 0;
        for (int i = 0; i < tokens.Count; i++)
        {
            (int token, string? text) = tokens[i];
            if (token == identifier && (text == "pg_temp" || text == schema) && Qualifies(i + 1))
            {
                return true;
            }

            bool castsTo = token == typecast || (token == KeywordToken && text == "as" && casts.TryPeek(out int castDepth) && castDepth == depth);
            if (castsTo && IsName(i + 1) && !Qualifies(i + 2) && IsTemporaryType(tokens[i + 1].Text!))
            {
                return true;
            }

            // A column's type taken from a table, as a function's argument's tt.v%TYPE, leaves no dependency on it.
            if (token == '%' && i + 1 < tokens.Count && tokens[i + 1] is (KeywordToken, "type") && IsName(i - 1))
            {
                List<string> names = [tokens[i - 1].Text!];
                for (int j = i - 2; Qualifies(j) && IsName(j - 1); j -= 2)
                {
                    names.Insert(0, tokens[j - 1].Text!);
                }

                if (names.Count > 1 && IsTemporaryTable(names[..^1]))
                {
                    return true;
                }
            }

            if (token == KeywordToken && text is "cast" or "treat" && i + 1 < tokens.Count && tokens[i + 1].Token == '(')
            {
                casts.Push(depth + 1);
            }
            else if (token == '(')
            {
                depth++;
            }
            else if (token == ')')
            {
                if (casts.TryPeek(out int closing) && closing == depth)
                {
                    _ = casts.Pop();
                }

                depth--;
            }
        }

        return false;
    }

    /// <summary>
    /// Splits text into PostgreSQL's tokens, with the text of each keyword and of each token of the given kinds, and
    /// every keyword as one token.
    /// </summary>
    private static List<(int Token, string? Text)> Lex(string text, int[] named)
    {
        ScanKeywordList* keywords = NativeGlobals.DangerousAddressOf_ScanKeywords();
        ushort[] keywordTokens = new ushort[keywords->num_keywords];
        Array.Fill(keywordTokens, KeywordToken);
        List<(int Token, string? Text)> tokens = [];
        fixed (ushort* tokenCodes = keywordTokens)
        fixed (byte* source = Worf.ToServerCString(text))
        {
            core_yy_extra_type extra;
            void* scanner = NativeMethods.scanner_init((sbyte*)source, &extra, keywords, tokenCodes);
            try
            {
#if !ANKUS_PG19
                // The parser has warned about the statement's escapes already.
                extra.escape_string_warning = false;
#endif
                core_YYSTYPE value;
                int location;
                for (int token = NativeMethods.core_yylex(&value, &location, scanner); token != 0;
                    token = NativeMethods.core_yylex(&value, &location, scanner))
                {
                    tokens.Add((token, token == KeywordToken ? Worf.FromServer(value.keyword)
                        : Array.IndexOf(named, token) >= 0 ? Worf.FromServer(value.str)
                        : null));
                }
            }
            finally
            {
                NativeMethods.scanner_finish(scanner);
            }
        }

        return tokens;
    }

    /// <summary>
    /// Gives each identifier written with Unicode escapes, <c>U&amp;"..."</c>, its name, as the parser does with the
    /// escape character that <c>UESCAPE</c> after it sets, and leaves string constants without text.
    /// </summary>
    private static List<(int Token, string? Text)> Names(List<(int Token, string? Text)> tokens, int identifier, int unicodeIdentifier,
        int stringConstant)
    {
        List<(int Token, string? Text)> names = [];
        for (int i = 0; i < tokens.Count; i++)
        {
            if (tokens[i].Token == unicodeIdentifier)
            {
                string text = tokens[i].Text!;
                char escape = '\\';
                if (i + 2 < tokens.Count && tokens[i + 1] is (KeywordToken, "uescape") && tokens[i + 2].Token == stringConstant &&
                    tokens[i + 2].Text is { Length: 1 } escapeText)
                {
                    escape = escapeText[0];
                    i += 2;
                }

                names.Add((identifier, Unescape(text, escape)));
            }
            else
            {
                names.Add(tokens[i].Token == stringConstant ? (stringConstant, null) : tokens[i]);
            }
        }

        return names;
    }

    /// <summary>
    /// Replaces the Unicode escapes in an identifier, the escape character followed by four hexadecimal digits, or by
    /// <c>+</c> and six, with what they stand for, and a doubled escape character with one.
    /// </summary>
    private static string Unescape(string text, char escape)
    {
        StringBuilder name = new();
        for (int i = 0; i < text.Length; i++)
        {
            int digits = text[i] != escape || i + 1 >= text.Length ? 0 : text[i + 1] == '+' ? 6 : text[i + 1] == escape ? -1 : 4;
            int start = i + (digits == 6 ? 2 : 1);
            if (digits == -1)
            {
                name.Append(escape);
                i++;
            }
            else if (digits > 0 && start + digits <= text.Length &&
                int.TryParse(text.AsSpan(start, digits), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out int codePoint) &&
                codePoint <= 0x10FFFF && codePoint is < 0xD800 or > 0xDFFF)
            {
                name.Append(char.ConvertFromUtf32(codePoint));
                i = start + digits - 1;
            }
            else
            {
                name.Append(text[i]);
            }
        }

        return name.ToString();
    }

    /// <summary>
    /// Checks whether a table, by its name's parts, is temporary.
    /// </summary>
    private static bool IsTemporaryTable(List<string> names)
        => Worf.ReadScalar<bool>("SELECT coalesce((SELECT relpersistence = 't' FROM pg_catalog.pg_class WHERE oid = pg_catalog.to_regclass($1)), false)",
            SpiParameter.Create(string.Join('.', names.Select(Spi.QuoteIdentifier))));

    /// <summary>
    /// Checks whether a type named without a schema is temporary.
    /// </summary>
    private static bool IsTemporaryType(string name)
    {
        uint typid;
        fixed (byte* typname = Worf.ToServerCString(name))
        {
            typid = NativeMethods.TypenameGetTypid((sbyte*)typname);
        }

        ObjectAddress address = new() { classId = TypeRelationId, objectId = typid };
        return typid != 0 && NativeMethods.isTempNamespace(NativeMethods.get_object_namespace(&address));
    }

    /// <summary>
    /// Adds the objects that COMMENT, SECURITY LABEL and GRANT name to the changed objects, which the object access
    /// hook doesn't see.
    /// </summary>
    private static void AddTargets(Node* parsetree)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_CommentStmt:
                AddTarget(((CommentStmt*)parsetree)->objtype, ((CommentStmt*)parsetree)->@object);
                break;

            case NodeTag.T_SecLabelStmt:
                AddTarget(((SecLabelStmt*)parsetree)->objtype, ((SecLabelStmt*)parsetree)->@object);
                break;

            case NodeTag.T_GrantStmt when ((GrantStmt*)parsetree)->targtype == GrantTargetType.ACL_TARGET_OBJECT:
                GrantStmt* grant = (GrantStmt*)parsetree;
                List* objects = grant->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    Node* target = (Node*)objects->elements[i].ptr_value;
                    if (grant->objtype is ObjectType.OBJECT_FUNCTION or ObjectType.OBJECT_PROCEDURE or ObjectType.OBJECT_ROUTINE)
                    {
                        AddTarget(grant->objtype, target);
                    }
                    else if (grant->objtype is ObjectType.OBJECT_TYPE or ObjectType.OBJECT_DOMAIN)
                    {
                        AddTarget(grant->objtype, (Node*)NativeMethods.makeTypeNameFromNameList((List*)target));
                    }
                }

                break;

            default:
                break;
        }
    }

    private static void AddTarget(ObjectType type, Node* target)
    {
        RelationData* relation = null;
        ObjectAddress address = NativeMethods.get_object_address(type, target, &relation, (int)PgLockMode.AccessShare, true);
        if (relation is not null)
        {
            NativeMethods.relation_close(relation, (int)PgLockMode.None);
        }

        if (address.objectId != 0)
        {
            s_changedObjects.Add((address.classId, address.objectId, address.objectSubId));
        }
    }

    /// <summary>
    /// Gets the session's temporary namespace, or zero while it has none.
    /// </summary>
    private static uint TemporaryNamespace()
    {
        uint nspoid;
        uint toastoid;
        NativeMethods.GetTempNamespaceState(&nspoid, &toastoid);
        return nspoid;
    }
}
