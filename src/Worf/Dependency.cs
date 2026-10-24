using System.Globalization;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("depend-table", """
    CREATE TABLE worf.depend (
        classid oid NOT NULL,
        objid oid NOT NULL,
        objsubid integer NOT NULL,

        refclassid oid NOT NULL,
        refobjid oid NOT NULL,
        refobjsubid integer NOT NULL,

    	deptype "char" NOT NULL
    ) WITH (user_catalog_table=true);

    CREATE INDEX depend_depender_index ON worf.depend (classid, objid, objsubid);
    CREATE INDEX depend_reference_index ON worf.depend (refclassid, refobjid, refobjsubid);
    """)]

namespace Worf;

/// <summary>
/// Dependency handling. Most of the code here is taken from PostgreSQL's dependency.c, as the dependency handling
/// in postgres is sadly not extensible.
/// </summary>
internal static class Dependency
{
    /// <summary>
    /// <c>DEPENDENCY_NORMAL</c>.
    /// </summary>
    internal const sbyte DependencyNormal = (sbyte)'n';

    /// <summary>
    /// <c>DEPENDENCY_AUTO</c>.
    /// </summary>
    internal const sbyte DependencyAuto = (sbyte)'a';

    /// <summary>
    /// <c>DEPENDENCY_INTERNAL</c>.
    /// </summary>
    internal const sbyte DependencyInternal = (sbyte)'i';

    /// <summary>
    /// <c>DEPENDENCY_EXTENSION</c>.
    /// </summary>
    internal const sbyte DependencyExtension = (sbyte)'e';

    /// <summary>
    /// <c>DEPENDENCY_AUTO_EXTENSION</c>.
    /// </summary>
    internal const sbyte DependencyAutoExtension = (sbyte)'x';

    /// <summary>
    /// <c>DEPENDENCY_PIN</c>, which PostgreSQL 15 removed.
    /// </summary>
    internal const sbyte DependencyPin = (sbyte)'p';

    /// <summary>
    /// <c>RelationRelationId</c>, the oid of <c>pg_class</c>.
    /// </summary>
    internal const uint RelationRelationId = 1259;

    /// <summary>
    /// <c>ExtensionRelationId</c>, the oid of <c>pg_extension</c>.
    /// </summary>
    internal const uint ExtensionRelationId = 3079;

    // ObjectAddressExtra flag bits.
    private const int DepflagOriginal = 0x0001;     // an original deletion target
    private const int DepflagNormal = 0x0002;       // reached via normal dependency
    private const int DepflagAuto = 0x0004;         // reached via auto dependency
    private const int DepflagInternal = 0x0008;     // reached via internal dependency
    private const int DepflagExtension = 0x0010;    // reached via extension dependency
    private const int DepflagReverse = 0x0020;      // reverse internal/extension link

    private const int MaxReportedDeps = 100;

    private const int AccessExclusiveLock = 8;

    private const int RowExclusiveLock = 3;

    private const string CatalogDepend = "depend";

    private const string CatalogDependDependerIndex = "depend_depender_index";

    private const string CatalogDependReferenceIndex = "depend_reference_index";

    private const int NattsDepend = 7;
    private const short AnumDependClassid = 1;
    private const short AnumDependObjid = 2;
    private const short AnumDependObjsubid = 3;
    private const short AnumDependRefclassid = 4;
    private const short AnumDependRefobjid = 5;
    private const short AnumDependRefobjsubid = 6;
    private const short AnumDependDeptype = 7;

    private const ushort BtEqualStrategyNumber = 3;
    private const uint FOideq = 184;
    private const uint FInt4eq = 65;

    private const int Notice = 18;

    private const int Log = 15;

    /// <summary>
    /// Record a dependency between 2 objects via their respective objectAddress. The first argument is the
    /// dependent object, the second the one it references. This simply creates an entry in worf.depend, without
    /// any other processing.
    /// </summary>
    /// <param name="depender">The dependent object.</param>
    /// <param name="referenced">The referenced object.</param>
    /// <param name="behavior">The dependency type.</param>
    internal static void RecordDependencyOn(ObjectAddress depender, ObjectAddress referenced, sbyte behavior)
        => RecordMultipleDependencies(depender, [referenced], behavior);

    /// <summary>
    /// Record multiple dependencies (of the same kind) for a single dependent object.
    /// </summary>
    /// <param name="depender">The dependent object.</param>
    /// <param name="referenced">The referenced objects.</param>
    /// <param name="behavior">The dependency type.</param>
    internal static unsafe void RecordMultipleDependencies(ObjectAddress depender, IReadOnlyList<ObjectAddress> referenced, sbyte behavior)
    {
        if (referenced.Count == 0)
        {
            return;         // nothing to do
        }

        RelationData* dependDesc = NativeMethods.table_open(GetDependRelOid(), RowExclusiveLock);

        ulong* values = stackalloc ulong[NattsDepend];
        bool* nulls = stackalloc bool[NattsDepend];
        new Span<bool>(nulls, NattsDepend).Clear();

        foreach (ObjectAddress reference in referenced)
        {
            // Record the Dependency. Note we don't bother to check for duplicate dependencies; there's no harm in
            // them.
            values[AnumDependClassid - 1] = depender.classId;
            values[AnumDependObjid - 1] = depender.objectId;
            values[AnumDependObjsubid - 1] = unchecked((uint)depender.objectSubId);

            values[AnumDependRefclassid - 1] = reference.classId;
            values[AnumDependRefobjid - 1] = reference.objectId;
            values[AnumDependRefobjsubid - 1] = unchecked((uint)reference.objectSubId);

            values[AnumDependDeptype - 1] = unchecked((byte)behavior);

            HeapTupleData* tup = NativeMethods.heap_form_tuple(dependDesc->rd_att, values, nulls);

            NativeMethods.CatalogTupleInsert(dependDesc, tup);

            NativeMethods.heap_freetuple(tup);
        }

        NativeMethods.table_close(dependDesc, RowExclusiveLock);
    }

    /// <summary>
    /// Record dependencies on a set of objects referenced by an expression that refers to one relation, as
    /// <c>recordDependencyOnSingleRelExpr</c> does.
    /// </summary>
    /// <param name="depender">The dependent object.</param>
    /// <param name="expr">The expression.</param>
    /// <param name="relId">The relation the expression's Vars refer to.</param>
    /// <param name="behavior">The dependency type for other objects.</param>
    /// <param name="selfBehavior">The dependency type for the relation itself.</param>
    internal static unsafe void RecordDependencyOnSingleRelExpr(ObjectAddress depender, Node* expr, uint relId,
        sbyte behavior, sbyte selfBehavior)
    {
        List<ObjectAddress> addrs = [];

        // Scan the expression tree for referenceable objects
        FindExprReferences(expr, relId, addrs);

        // Remove any duplicates
        EliminateDuplicateDependencies(addrs);

        // Separate self-dependencies if necessary
        if (behavior != selfBehavior && addrs.Count > 0)
        {
            List<ObjectAddress> selfAddrs = addrs.FindAll(thisobj => thisobj.classId == RelationRelationId && thisobj.objectId == relId);
            _ = addrs.RemoveAll(thisobj => thisobj.classId == RelationRelationId && thisobj.objectId == relId);

            // Record the self-dependencies
            RecordMultipleDependencies(depender, selfAddrs, selfBehavior);
        }

        // Record the external dependencies
        RecordMultipleDependencies(depender, addrs, behavior);
    }

    /// <summary>
    /// Drop dependencies if possible, error if not.
    /// </summary>
    /// <param name="obj">The object whose dependents to drop.</param>
    /// <param name="cascade">Whether to cascade, rather than restrict, the drop.</param>
    internal static void TryDropDependencies(ObjectAddress obj, bool cascade)
    {
        // Construct a list of objects to delete (ie, the given object plus everything directly or indirectly
        // dependent on it).
        ObjectAddresses targetObjects = new();

        FindDependentObjects(obj, DepflagOriginal, null, targetObjects, null);

        // Check if deletion is allowed, and report about cascaded deletes.
        ReportDependentObjects(targetObjects, cascade, PgLogLevel.Notice, obj);

        // Unlike the builtin dependency tracking, we don't actually drop the original object here as it has
        // already been dropped by the time this function has been called so remove it from the array.
        if (targetObjects.Refs.Count != 0)
        {
            targetObjects.Refs.RemoveAt(targetObjects.Refs.Count - 1);
            targetObjects.Extras.RemoveAt(targetObjects.Extras.Count - 1);
        }

        DeleteObjectsInList(targetObjects);

        DeleteOneObjectDepencencyRecord(obj);

        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    /// <summary>
    /// Checks the dependencies of a relation being dropped and forgets its synchronization status.
    /// </summary>
    /// <param name="obj">The object being dropped.</param>
    /// <param name="behavior">Whether the drop cascades.</param>
    internal static void CheckDependency(ObjectAddress obj, DropBehavior behavior)
    {
        if (obj.classId != RelationRelationId)
        {
            return;
        }

        TryDropDependencies(obj, behavior == DropBehavior.DROP_CASCADE);

        unsafe
        {
            // get_rel_relkind returns no kind for a relation that isn't there.
            if (NativeMethods.get_rel_relkind(obj.objectId) == (sbyte)'r')
            {
                Sync.DropTableSyncStatus(Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(obj.objectId))),
                    Worf.FromServer(NativeMethods.get_rel_name(obj.objectId)));
            }
        }
    }

    /// <summary>
    /// Creates an object address.
    /// </summary>
    /// <param name="classId">The catalog containing the object.</param>
    /// <param name="objectId">The object.</param>
    /// <param name="objectSubId">The column number, or 0.</param>
    /// <returns>The address.</returns>
    internal static ObjectAddress Address(uint classId, uint objectId, int objectSubId)
        => new() { classId = classId, objectId = objectId, objectSubId = objectSubId };

    /// <summary>
    /// Go through the objects given running the final actions on them, and execute the actual deletion.
    /// </summary>
    private static void DeleteObjectsInList(ObjectAddresses targetObjects)
    {
        // Delete all the objects in the proper order.
        foreach (ObjectAddress thisobj in targetObjects.Refs)
        {
            DeleteOneObject(thisobj);
        }
    }

    /// <summary>
    /// Find all objects that depend on 'object'. For every object that depends on the starting object, acquire a
    /// deletion lock on the object, add it to targetObjects (if not already there), and recursively find objects
    /// that depend on it. An object's dependencies will be placed into targetObjects before the object itself;
    /// this means that the finished list's order represents a safe deletion order.
    /// </summary>
    /// <param name="obj">The object to add to targetObjects and find dependencies on.</param>
    /// <param name="flags">Flags to be ORed into the object's targetObjects entry.</param>
    /// <param name="stack">List of objects being visited in current recursion; topmost item is the object that
    /// we recursed from (null for external callers).</param>
    /// <param name="targetObjects">List of objects that are scheduled to be deleted.</param>
    /// <param name="pendingObjects">List of other objects slated for destruction, but not necessarily in
    /// targetObjects yet (can be null if none).</param>
    private static void FindDependentObjects(ObjectAddress obj, int flags, ObjectAddressStack? stack,
        ObjectAddresses targetObjects, ObjectAddresses? pendingObjects)
    {
        // If the target object is already being visited in an outer recursion level, just report the current flags
        // back to that level and exit. This is needed to avoid infinite recursion in the face of circular
        // dependencies.
        if (StackAddressPresentAddFlags(obj, flags, stack))
        {
            return;
        }

        // It's also possible that the target object has already been completely processed and put into
        // targetObjects. If so, again we just add the specified flags to its entry and return.
        if (ObjectAddressPresentAddFlags(obj, flags, targetObjects))
        {
            return;
        }

        // The target object might be internally dependent on some other object (its "owner"), and/or be a member
        // of an extension (also considered its owner). If so, and if we aren't recursing from the owning object,
        // we have to transform this deletion request into a deletion request of the owning object. The way to find
        // out about this is to scan the worf.depend entries that show what this object depends on.
        foreach (DependRow foundDep in ScanDepend(true, obj))
        {
            ObjectAddress otherObject = Address(foundDep.ClassId, foundDep.ObjectId, foundDep.ObjectSubId);

            switch (foundDep.DepType)
            {
                case DependencyNormal:
                case DependencyAuto:
                case DependencyAutoExtension:
                    // no problem
                    break;
                case DependencyInternal:
                case DependencyExtension:
                    // This object is part of the internal implementation of another object, or is part of the
                    // extension that is the other object. We have three cases:
                    //
                    // 1. At the outermost recursion level, we normally disallow the DROP. However, there are
                    // exceptions.
                    if (stack is null)
                    {
                        // Exception 1a: if the owning object is listed in pendingObjects, just release the
                        // caller's lock and return. We'll eventually complete the DROP when we reach that entry in
                        // the pending list.
                        if (pendingObjects is not null && ObjectAddressPresent(otherObject, pendingObjects))
                        {
                            // need to release caller's lock; see notes below
                            ReleaseDeletionLock(obj);
                            return;
                        }

                        // Exception 1b: if the owning object is the extension currently being created/altered,
                        // it's okay to continue with the deletion. Note that worf currently does not care about
                        // extension dependencies, so we relax this and just skip any extension dependencies.
                        bool creatingExtension;
                        unsafe
                        {
                            creatingExtension = NativeGlobals.creating_extension;
                        }

                        if (creatingExtension && otherObject.classId == ExtensionRelationId)
                        {
                            break;
                        }

                        // No exception applies, so throw the error
                        string otherObjDesc = GetObjectDescription(otherObject);
                        throw new PgException(PgSqlStates.DependentObjectsStillExist,
                            $"cannot drop {GetObjectDescription(obj)} because {otherObjDesc} requires it",
                            hint: $"You can drop {otherObjDesc} instead.");
                    }

                    // 2. When recursing from the other end of this dependency, it's okay to continue with the
                    // deletion. This holds when recursing from a whole object that includes the nominal other end
                    // as a component, too. Since there can be more than one "owning" object, we have to allow
                    // matches that are more than one level down in the stack.
                    if (StackAddressPresentAddFlags(otherObject, 0, stack))
                    {
                        break;
                    }

                    // 3. Not all the owning objects have been visited, so transform this deletion request into a
                    // delete of this owning object.
                    //
                    // First, release caller's lock on this object and get deletion lock on the owning object. (We
                    // must release caller's lock to avoid deadlock against a concurrent deletion of the owning
                    // object.)
                    ReleaseDeletionLock(obj);
                    AcquireDeletionLock(otherObject);

                    // The owning object might have been deleted while we waited to lock it; if so, neither it nor
                    // the current object are interesting anymore. We test this by checking the worf.depend entry.
                    if (!RecheckDepend(foundDep))
                    {
                        ReleaseDeletionLock(otherObject);
                        return;
                    }

                    // Okay, recurse to the owning object instead of proceeding. We do not need to stack the current
                    // object; we want the traversal order to be as if the original reference had linked to the
                    // owning object instead of this one. The dependency type is a "reverse" dependency: we need to
                    // delete the owning object if this one is to be deleted, but this linkage is never a reason for
                    // an automatic deletion.
                    FindDependentObjects(otherObject, DepflagReverse, stack, targetObjects, pendingObjects);

                    // And we're done here.
                    return;
#if ANKUS_PG13 || ANKUS_PG14
                case DependencyPin:
                    // Should not happen; PIN dependencies should have zeroes in the depender fields...
                    throw new PgException(PgSqlStates.InternalError,
                        $"incorrect use of PIN dependency with {GetObjectDescription(obj)}");
#endif
                default:
                    throw new PgException(PgSqlStates.InternalError,
                        $"unrecognized dependency type '{(char)foundDep.DepType}' for {GetObjectDescription(obj)}");
            }
        }

        // Now recurse to any dependent objects. We must visit them first since they have to be deleted before the
        // current object.
        ObjectAddressStack mystack = new(obj, flags, stack);

        foreach (DependRow foundDep in ScanDepend(false, obj))
        {
            ObjectAddress otherObject = Address(foundDep.ClassId, foundDep.ObjectId, foundDep.ObjectSubId);

            // Must lock the dependent object before recursing to it.
            AcquireDeletionLock(otherObject);

            // The dependent object might have been deleted while we waited to lock it; if so, we don't need to do
            // anything more with it. We can test this cheaply and independently of the object's type by seeing if
            // the worf.depend tuple we are looking at is still live. (If the object got deleted, the tuple would
            // have been deleted too.)
            if (!RecheckDepend(foundDep))
            {
                // release the now-useless lock
                ReleaseDeletionLock(otherObject);

                // and continue scanning for dependencies
                continue;
            }

            // Recurse, passing flags indicating the dependency type
            int subflags = foundDep.DepType switch
            {
                DependencyNormal => DepflagNormal,
                DependencyAuto or DependencyAutoExtension => DepflagAuto,
                DependencyInternal => DepflagInternal,
                DependencyExtension => DepflagExtension,
#if ANKUS_PG13 || ANKUS_PG14
                // For a PIN dependency we just ereport immediately; there won't be any others to report.
                DependencyPin => throw new PgException(PgSqlStates.DependentObjectsStillExist,
                    $"cannot drop {GetObjectDescription(obj)} because it is required by the database system"),
#endif
                _ => throw new PgException(PgSqlStates.InternalError,
                    $"unrecognized dependency type '{(char)foundDep.DepType}' for {GetObjectDescription(obj)}"),
            };

            FindDependentObjects(otherObject, subflags, mystack, targetObjects, pendingObjects);
        }

        // Finally, we can add the target object to targetObjects. Be careful to include any flags that were passed
        // back down to us from inner recursion levels.
        ObjectAddressExtra extra = new(mystack.Flags, stack is not null ? stack.Object : default);
        targetObjects.Refs.Add(obj);
        targetObjects.Extras.Add(extra);
    }

    /// <summary>
    /// Report about dependencies, and fail if RESTRICT. Tell the user about dependent objects that we are going to
    /// delete (or would need to delete, but are prevented by RESTRICT mode); then error out if there are any and
    /// it's not CASCADE mode.
    /// </summary>
    /// <param name="targetObjects">List of objects that are scheduled to be deleted.</param>
    /// <param name="cascade">Whether the drop cascades.</param>
    /// <param name="msglevel">Log level for non-error report messages, which is always NOTICE.</param>
    /// <param name="origObject">Base object of deletion.</param>
    private static void ReportDependentObjects(ObjectAddresses targetObjects, bool cascade, PgLogLevel msglevel, ObjectAddress origObject)
    {
        bool ok = true;
        StringBuilder clientdetail = new();
        StringBuilder logdetail = new();
        int numReportedClient = 0;
        int numNotReportedClient = 0;

        // This is cludge for Windows (Postgres does not define the GUC variables as PGDLLIMPORT): the settings are
        // read through their text, which atoi turns into zero.
        int myClientMinMessages = Atoi(Worf.GetConfigOptionByName("client_min_messages"));
        int myLogMinMessages = Atoi(Worf.GetConfigOptionByName("log_min_messages"));

        // If no error is to be thrown, and the msglevel is too low to be shown to either client or server log,
        // there's no need to do any of the work.
        if (cascade && Notice < myClientMinMessages && (Notice < myLogMinMessages || myLogMinMessages == Log))
        {
            return;
        }

        // We process the list back to front (ie, in dependency order not deletion order), since this makes for a
        // more understandable display.
        for (int i = targetObjects.Refs.Count - 1; i >= 0; i--)
        {
            ObjectAddress obj = targetObjects.Refs[i];
            ObjectAddressExtra extra = targetObjects.Extras[i];

            // Ignore the original deletion target(s)
            if ((extra.Flags & DepflagOriginal) != 0)
            {
                continue;
            }

            string objDesc = GetObjectDescription(obj);

            // If, at any stage of the recursive search, we reached the object via an AUTO, INTERNAL, or EXTENSION
            // dependency, then it's okay to delete it even in RESTRICT mode.
            if ((extra.Flags & (DepflagAuto | DepflagInternal | DepflagExtension)) != 0)
            {
                // auto-cascades are reported at DEBUG2, not msglevel.
                PgLog.Debug2($"drop auto-cascades to {objDesc}");
            }
            else if (!cascade)
            {
                string otherDesc = GetObjectDescription(extra.Dependee);

                if (numReportedClient < MaxReportedDeps)
                {
                    // separate entries with a newline
                    if (clientdetail.Length != 0)
                    {
                        clientdetail.Append('\n');
                    }

                    clientdetail.Append(CultureInfo.InvariantCulture, $"{objDesc} depends on {otherDesc}");
                    numReportedClient++;
                }
                else
                {
                    numNotReportedClient++;
                }

                // separate entries with a newline
                if (logdetail.Length != 0)
                {
                    logdetail.Append('\n');
                }

                logdetail.Append(CultureInfo.InvariantCulture, $"{objDesc} depends on {otherDesc}");
                ok = false;
            }
            else
            {
                if (numReportedClient < MaxReportedDeps)
                {
                    // separate entries with a newline
                    if (clientdetail.Length != 0)
                    {
                        clientdetail.Append('\n');
                    }

                    clientdetail.Append(CultureInfo.InvariantCulture, $"drop cascades to {objDesc}");
                    numReportedClient++;
                }
                else
                {
                    numNotReportedClient++;
                }

                // separate entries with a newline
                if (logdetail.Length != 0)
                {
                    logdetail.Append('\n');
                }

                logdetail.Append(CultureInfo.InvariantCulture, $"drop cascades to {objDesc}");
            }
        }

        if (numNotReportedClient > 0)
        {
            if (numNotReportedClient == 1)
            {
                clientdetail.Append(CultureInfo.InvariantCulture, $"\nand {numNotReportedClient} other object (see server log for list)");
            }
            else
            {
                clientdetail.Append(CultureInfo.InvariantCulture, $"\nand {numNotReportedClient} other objects (see server log for list)");
            }
        }

        if (!ok)
        {
            throw new PgException(PgSqlStates.DependentObjectsStillExist,
                $"cannot drop {GetObjectDescription(origObject)} because other objects depend on it",
                detail: clientdetail.ToString(), hint: "Use DROP ... CASCADE to drop the dependent objects too.")
            {
                DetailLog = logdetail.ToString(),
            };
        }
        else if (numReportedClient > 1)
        {
            int total = numReportedClient + numNotReportedClient;
            PgLog.Write(msglevel, new PgDiagnostic(total == 1
                ? $"drop cascades to {total} other object"
                : $"drop cascades to {total} other objects")
            {
                Detail = clientdetail.ToString(),
                DetailLog = logdetail.ToString(),
            });
        }
        else if (numReportedClient == 1)
        {
            // we just use the single item as-is
            PgLog.Write(msglevel, clientdetail.ToString());
        }
    }

    /// <summary>
    /// Acquire a suitable lock for deleting an object. We use LockRelation for relations, LockDatabaseObject for
    /// everything else.
    /// </summary>
    private static void AcquireDeletionLock(ObjectAddress obj)
    {
        unsafe
        {
            if (obj.classId == RelationRelationId)
            {
                NativeMethods.LockRelationOid(obj.objectId, AccessExclusiveLock);
            }
            else
            {
                // assume we should lock the whole object not a sub-object
                NativeMethods.LockDatabaseObject(obj.classId, obj.objectId, 0, AccessExclusiveLock);
            }
        }
    }

    /// <summary>
    /// Release an object deletion lock.
    /// </summary>
    private static void ReleaseDeletionLock(ObjectAddress obj)
    {
        unsafe
        {
            if (obj.classId == RelationRelationId)
            {
                NativeMethods.UnlockRelationOid(obj.objectId, AccessExclusiveLock);
            }
            else
            {
                // assume we should lock the whole object not a sub-object
                NativeMethods.UnlockDatabaseObject(obj.classId, obj.objectId, 0, AccessExclusiveLock);
            }
        }
    }

    /// <summary>
    /// Test whether an object is present in an ObjectAddresses array.
    /// </summary>
    private static bool ObjectAddressPresent(ObjectAddress obj, ObjectAddresses addrs)
    {
        for (int i = addrs.Refs.Count - 1; i >= 0; i--)
        {
            ObjectAddress thisobj = addrs.Refs[i];
            if (obj.classId == thisobj.classId && obj.objectId == thisobj.objectId &&
                (obj.objectSubId == thisobj.objectSubId || thisobj.objectSubId == 0))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// As <see cref="ObjectAddressPresent"/>, except that if the object is present then also OR the given flags
    /// into its associated extra data (which must exist).
    /// </summary>
    private static bool ObjectAddressPresentAddFlags(ObjectAddress obj, int flags, ObjectAddresses addrs)
    {
        bool result = false;

        for (int i = addrs.Refs.Count - 1; i >= 0; i--)
        {
            ObjectAddress thisobj = addrs.Refs[i];

            if (obj.classId == thisobj.classId && obj.objectId == thisobj.objectId)
            {
                if (obj.objectSubId == thisobj.objectSubId)
                {
                    addrs.Extras[i] = addrs.Extras[i] with { Flags = addrs.Extras[i].Flags | flags };
                    result = true;
                }
                else if (thisobj.objectSubId == 0)
                {
                    // We get here if we find a need to delete a column after having already decided to drop its
                    // whole table. Obviously we no longer need to drop the subobject, so report that we found the
                    // subobject in the array. But don't plaster its flags on the whole object.
                    result = true;
                }
                else if (obj.objectSubId == 0)
                {
                    // We get here if we find a need to delete a whole table after having already decided to drop
                    // one of its columns. We can't report that the whole object is in the array, but we should
                    // mark the subobject with the whole object's flags. Because there could be other subobjects of
                    // this object in the array, this case means we always have to loop through the whole array; we
                    // cannot exit early on a match.
                    addrs.Extras[i] = addrs.Extras[i] with { Flags = addrs.Extras[i].Flags | flags };
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Similar to <see cref="ObjectAddressPresentAddFlags"/>, except we search an ObjectAddressStack.
    /// </summary>
    private static bool StackAddressPresentAddFlags(ObjectAddress obj, int flags, ObjectAddressStack? stack)
    {
        bool result = false;

        for (ObjectAddressStack? stackptr = stack; stackptr is not null; stackptr = stackptr.Next)
        {
            ObjectAddress thisobj = stackptr.Object;

            if (obj.classId == thisobj.classId && obj.objectId == thisobj.objectId)
            {
                if (obj.objectSubId == thisobj.objectSubId)
                {
                    stackptr.Flags |= flags;
                    result = true;
                }
                else if (thisobj.objectSubId == 0)
                {
                    // We're visiting a column with whole table already on stack. As in
                    // ObjectAddressPresentAddFlags(), we can skip further processing of the subobject, but we don't
                    // want to propagate flags for the subobject to the whole object.
                    result = true;
                }
                else if (obj.objectSubId == 0)
                {
                    // We're visiting a table with column already on stack. As in ObjectAddressPresentAddFlags(), we
                    // should propagate flags for the whole object to each of its subobjects.
                    stackptr.Flags |= flags;
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Remove any dependency records for the given object.
    /// </summary>
    private static unsafe void DeleteOneObjectDepencencyRecord(ObjectAddress obj)
    {
        RelationData* depRel = NativeMethods.table_open(GetDependRelOid(), RowExclusiveLock);

        ScanKeyData* key = stackalloc ScanKeyData[3];
        int nkeys = InitDependKeys(key, AnumDependClassid, AnumDependObjid, AnumDependObjsubid, obj);

        SysScanDescData* scan = NativeMethods.systable_beginscan(depRel, GetDependIndexOid(true), true, null, nkeys, key);

        HeapTupleData* tup;
        while ((tup = NativeMethods.systable_getnext(scan)) is not null)
        {
            NativeMethods.simple_heap_delete(depRel, &tup->t_self);
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(depRel, RowExclusiveLock);
    }

    /// <summary>
    /// Delete a single object.
    /// </summary>
    private static void DeleteOneObject(ObjectAddress obj)
    {
        // Delete the object itself, in an object-type-dependent way.
        DoDeletion(obj);

        // Now remove any worf.depend records that link from this object to others. (Any records linking to this
        // object should be gone already.) When dropping a whole object (subId = 0), remove all worf.depend records
        // for its sub-objects too.
        DeleteOneObjectDepencencyRecord(obj);

        // CommandCounterIncrement here to ensure that preceding changes are all visible to the next deletion step.
        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    /// <summary>
    /// Actually delete a single object. This is customised in worf.
    /// </summary>
    private static void DoDeletion(ObjectAddress obj)
    {
        if (obj.classId == ReplicationSet.GetReplicationSetRelOid())
        {
            ReplicationSet.DropReplicationSet(obj.objectId);
        }
        else if (obj.classId == ReplicationSet.GetReplicationSetTableRelOid())
        {
            ReplicationSet.ReplicationSetRemoveTable(obj.objectId, unchecked((uint)obj.objectSubId), true);
        }
        else if (obj.classId == ReplicationSet.GetReplicationSetSeqRelOid())
        {
            ReplicationSet.ReplicationSetRemoveSeq(obj.objectId, unchecked((uint)obj.objectSubId), true);
        }
        else
        {
            throw new PgException(PgSqlStates.InternalError, $"unrecognized worf object class: {obj.classId}");
        }
    }

    /// <summary>
    /// Get the object description, first looking into our own knowledge of worf objects and finally standard
    /// postgres way.
    /// </summary>
    private static string GetObjectDescription(ObjectAddress obj)
    {
        if (obj.classId == ReplicationSet.GetReplicationSetRelOid())
        {
            WorfRepSet repset = ReplicationSet.GetReplicationSet(obj.objectId);
            return $"replication set {repset.Name}";
        }
        else if (obj.classId == ReplicationSet.GetReplicationSetTableRelOid() ||
                 obj.classId == ReplicationSet.GetReplicationSetSeqRelOid())
        {
            ObjectAddress tbladdr = Address(RelationRelationId, unchecked((uint)obj.objectSubId), 0);
            WorfRepSet repset = ReplicationSet.GetReplicationSet(obj.objectId);
            return $"{GetObjectDescription(tbladdr)} membership in replication set {repset.Name}";
        }

        unsafe
        {
#if ANKUS_PG13
            sbyte* description = NativeMethods.getObjectDescription(&obj);
#else
            sbyte* description = NativeMethods.getObjectDescription(&obj, false);
#endif
            string result = Worf.FromServer(description);
            NativeMethods.pfree(description);
            return result;
        }
    }

    /// <summary>
    /// Scans the worf.depend rows that record what an object depends on, or what depends on it, returning the
    /// other end of each.
    /// </summary>
    /// <param name="depender">Whether the object is the dependent end.</param>
    /// <param name="obj">The object.</param>
    private static unsafe List<DependRow> ScanDepend(bool depender, ObjectAddress obj)
    {
        RelationData* depRel = NativeMethods.table_open(GetDependRelOid(), RowExclusiveLock);

        ScanKeyData* key = stackalloc ScanKeyData[3];
        int nkeys = depender
            ? InitDependKeys(key, AnumDependClassid, AnumDependObjid, AnumDependObjsubid, obj)
            : InitDependKeys(key, AnumDependRefclassid, AnumDependRefobjid, AnumDependRefobjsubid, obj);

        SysScanDescData* scan = NativeMethods.systable_beginscan(depRel, GetDependIndexOid(depender), true, null, nkeys, key);

        // The other end of each dependency.
        short classid = depender ? AnumDependRefclassid : AnumDependClassid;
        short objid = depender ? AnumDependRefobjid : AnumDependObjid;
        short objsubid = depender ? AnumDependRefobjsubid : AnumDependObjsubid;

        List<DependRow> rows = [];
        HeapTupleData* tup;
        while ((tup = NativeMethods.systable_getnext(scan)) is not null)
        {
            TupleDescData* desc = depRel->rd_att;
            bool isnull;
            rows.Add(new DependRow(tup->t_self,
                (uint)NativeMethods.heap_getattr(tup, classid, desc, &isnull),
                (uint)NativeMethods.heap_getattr(tup, objid, desc, &isnull),
                unchecked((int)NativeMethods.heap_getattr(tup, objsubid, desc, &isnull)),
                unchecked((sbyte)NativeMethods.heap_getattr(tup, AnumDependDeptype, desc, &isnull))));
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(depRel, RowExclusiveLock);

        return rows;
    }

    /// <summary>
    /// Checks that a worf.depend row is still live, as systable_recheck_tuple does.
    /// </summary>
    private static unsafe bool RecheckDepend(DependRow row)
    {
        RelationData* depRel = NativeMethods.table_open(GetDependRelOid(), RowExclusiveLock);

        HeapTupleData tuple = default;
        tuple.t_self = row.Ctid;
        int buffer;
        SnapshotData* freshsnap = NativeMethods.RegisterSnapshot(NativeMethods.GetLatestSnapshot());
#if ANKUS_PG13 || ANKUS_PG14
        bool found = NativeMethods.heap_fetch(depRel, freshsnap, &tuple, &buffer);
#else
        bool found = NativeMethods.heap_fetch(depRel, freshsnap, &tuple, &buffer, false);
#endif
        if (found)
        {
            NativeMethods.ReleaseBuffer(buffer);
        }

        NativeMethods.UnregisterSnapshot(freshsnap);

        NativeMethods.table_close(depRel, RowExclusiveLock);

        return found;
    }

    /// <summary>
    /// Sets up the keys that find an object's rows in worf.depend, by its depender or referenced columns.
    /// </summary>
    /// <returns>The number of keys.</returns>
    private static unsafe int InitDependKeys(ScanKeyData* key, short anumClassid, short anumObjid, short anumObjsubid, ObjectAddress obj)
    {
        NativeMethods.ScanKeyInit(&key[0], anumClassid, BtEqualStrategyNumber, FOideq, obj.classId);
        NativeMethods.ScanKeyInit(&key[1], anumObjid, BtEqualStrategyNumber, FOideq, obj.objectId);
        if (obj.objectSubId != 0)
        {
            NativeMethods.ScanKeyInit(&key[2], anumObjsubid, BtEqualStrategyNumber, FInt4eq, unchecked((uint)obj.objectSubId));
            return 3;
        }

        return 2;
    }

    /// <summary>
    /// Gets the oid of the dependency catalog.
    /// </summary>
    private static uint GetDependRelOid() => Worf.GetWorfTableOid(CatalogDepend);

    /// <summary>
    /// Gets the oid of the dependency catalog's index on its depender or referenced columns. pglogical scans the
    /// whole catalog instead, which makes dropping many replicated tables in one transaction take quadratic time.
    /// </summary>
    /// <param name="depender">Whether to get the index on the depender columns.</param>
    private static uint GetDependIndexOid(bool depender) =>
        Worf.GetWorfTableOid(depender ? CatalogDependDependerIndex : CatalogDependReferenceIndex);

    private static int Atoi(string? value)
    {
        int end = 0;
        while (value is not null && end < value.Length && (char.IsAsciiDigit(value[end]) || (end == 0 && value[end] is '-' or '+')))
        {
            end++;
        }

        return value is not null && int.TryParse(value.AsSpan(0, end), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int parsed)
            ? parsed : 0;
    }

    // Catalogs of the object classes expressions refer to.
    private const uint TypeRelationId = 1247;
    private const uint ProcedureRelationId = 1255;
    private const uint ConstraintRelationId = 2606;
    private const uint NamespaceRelationId = 2615;
    private const uint OperatorRelationId = 2617;
    private const uint OperatorFamilyRelationId = 2753;
    private const uint CollationRelationId = 3456;
    private const uint TsDictionaryRelationId = 3600;
    private const uint TsConfigRelationId = 3602;
    private const uint DefaultCollationOid = 100;

    /// <summary>
    /// Recursively search an expression tree of a single relation for object references, as
    /// <c>find_expr_references_walker</c> does for the expressions a row filter can hold.
    /// </summary>
    private static unsafe void FindExprReferences(Node* node, uint relId, List<ObjectAddress> addrs)
    {
        if (node is null)
        {
            return;
        }

        switch (node->type)
        {
            case NodeTag.T_Var:
            {
                Var* var = (Var*)node;

                // Find matching rtable entry, or complain if not found
                if (var->varlevelsup >= 1)
                {
                    throw new PgException(PgSqlStates.InternalError, $"invalid varlevelsup {var->varlevelsup}");
                }

                if (var->varno != 1)
                {
                    throw new PgException(PgSqlStates.InternalError, $"invalid varno {var->varno}");
                }

                // A whole-row Var references no specific columns, so adds no new dependency.
                if (var->varattno == 0)
                {
                    return;
                }

                // If it's a plain relation, reference this column
                addrs.Add(Address(RelationRelationId, relId, var->varattno));
                return;
            }

            case NodeTag.T_Const:
            {
                Const* con = (Const*)node;

                // A constant must depend on the constant's datatype
                addrs.Add(Address(TypeRelationId, con->consttype, 0));

                // We must also depend on the constant's collation: it could be different from the datatype's, if a
                // CollateExpr was const-folded to a simple constant.
                if (con->constcollid != 0 && con->constcollid != DefaultCollationOid)
                {
                    addrs.Add(Address(CollationRelationId, con->constcollid, 0));
                }

                // If it's a regclass or similar literal referring to an existing object, add a reference to that
                // object.
                if (!con->constisnull)
                {
                    uint objoid = (uint)con->constvalue;
                    switch (con->consttype)
                    {
                        case 24:    // REGPROCOID
                        case 2202:  // REGPROCEDUREOID
                            AddIfExists("pg_proc", ProcedureRelationId, objoid, addrs);
                            break;
                        case 2203:  // REGOPEROID
                        case 2204:  // REGOPERATOROID
                            AddIfExists("pg_operator", OperatorRelationId, objoid, addrs);
                            break;
                        case 2205:  // REGCLASSOID
                            AddIfExists("pg_class", RelationRelationId, objoid, addrs);
                            break;
                        case 2206:  // REGTYPEOID
                            AddIfExists("pg_type", TypeRelationId, objoid, addrs);
                            break;
                        case 3734:  // REGCONFIGOID
                            AddIfExists("pg_ts_config", TsConfigRelationId, objoid, addrs);
                            break;
                        case 3769:  // REGDICTIONARYOID
                            AddIfExists("pg_ts_dict", TsDictionaryRelationId, objoid, addrs);
                            break;
                        case 4089:  // REGNAMESPACEOID
                            AddIfExists("pg_namespace", NamespaceRelationId, objoid, addrs);
                            break;

                        // Dependencies for regrole should be shared among all databases, so explicitly inhibit to
                        // have dependencies.
                        case 4096:  // REGROLEOID
                            throw new PgException(PgSqlStates.FeatureNotSupported, "constant of the type \"regrole\" cannot be used here");
                        default:
                            break;
                    }
                }

                return;
            }

            case NodeTag.T_Param:
            {
                Param* param = (Param*)node;

                // A parameter must depend on the parameter's datatype
                addrs.Add(Address(TypeRelationId, param->paramtype, 0));

                // and its collation, just as for Consts
                if (param->paramcollid != 0 && param->paramcollid != DefaultCollationOid)
                {
                    addrs.Add(Address(CollationRelationId, param->paramcollid, 0));
                }

                return;
            }

            case NodeTag.T_FuncExpr:
            {
                FuncExpr* funcexpr = (FuncExpr*)node;
                addrs.Add(Address(ProcedureRelationId, funcexpr->funcid, 0));
                FindListReferences(funcexpr->args, relId, addrs);
                return;
            }

            case NodeTag.T_OpExpr:
            case NodeTag.T_DistinctExpr:
            case NodeTag.T_NullIfExpr:
            {
                OpExpr* opexpr = (OpExpr*)node;
                addrs.Add(Address(OperatorRelationId, opexpr->opno, 0));
                FindListReferences(opexpr->args, relId, addrs);
                return;
            }

            case NodeTag.T_ScalarArrayOpExpr:
            {
                ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
                addrs.Add(Address(OperatorRelationId, opexpr->opno, 0));
                FindListReferences(opexpr->args, relId, addrs);
                return;
            }

            case NodeTag.T_SubPlan:
                // Extra work needed here if we ever need this case
                throw new PgException(PgSqlStates.InternalError, "already-planned subqueries not supported");

            case NodeTag.T_FieldSelect:
            {
                FieldSelect* fselect = (FieldSelect*)node;
                uint argtype = NativeMethods.getBaseType(NativeMethods.exprType((Node*)fselect->arg));
                uint reltype = NativeMethods.get_typ_typrelid(argtype);

                // We need a dependency on the specific column named in FieldSelect, assuming we can identify the
                // pg_class OID for it. If we can't, make a dependency on the type, as it might not appear anywhere
                // else in the expression.
                addrs.Add(reltype != 0
                    ? Address(RelationRelationId, reltype, fselect->fieldnum)
                    : Address(TypeRelationId, fselect->resulttype, 0));

                // the collation might not be referenced anywhere else, either
                if (fselect->resultcollid != 0 && fselect->resultcollid != DefaultCollationOid)
                {
                    addrs.Add(Address(CollationRelationId, fselect->resultcollid, 0));
                }

                FindExprReferences((Node*)fselect->arg, relId, addrs);
                return;
            }

            case NodeTag.T_FieldStore:
            {
                FieldStore* fstore = (FieldStore*)node;
                uint reltype = NativeMethods.get_typ_typrelid(fstore->resulttype);

                // similar considerations to FieldSelect, but multiple column(s)
                if (reltype != 0)
                {
                    for (int i = 0; fstore->fieldnums is not null && i < fstore->fieldnums->length; i++)
                    {
                        addrs.Add(Address(RelationRelationId, reltype, fstore->fieldnums->elements[i].int_value));
                    }
                }
                else
                {
                    addrs.Add(Address(TypeRelationId, fstore->resulttype, 0));
                }

                FindExprReferences((Node*)fstore->arg, relId, addrs);
                FindListReferences(fstore->newvals, relId, addrs);
                return;
            }

            case NodeTag.T_RelabelType:
            {
                RelabelType* relab = (RelabelType*)node;

                // since there is no function dependency, need to depend on type
                addrs.Add(Address(TypeRelationId, relab->resulttype, 0));

                // the collation might not be referenced anywhere else, either
                if (relab->resultcollid != 0 && relab->resultcollid != DefaultCollationOid)
                {
                    addrs.Add(Address(CollationRelationId, relab->resultcollid, 0));
                }

                FindExprReferences((Node*)relab->arg, relId, addrs);
                return;
            }

            case NodeTag.T_CoerceViaIO:
            {
                CoerceViaIO* iocoerce = (CoerceViaIO*)node;

                // since there is no exposed function, need to depend on type
                addrs.Add(Address(TypeRelationId, iocoerce->resulttype, 0));
                FindExprReferences((Node*)iocoerce->arg, relId, addrs);
                return;
            }

            case NodeTag.T_ArrayCoerceExpr:
            {
                ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;

                addrs.Add(Address(TypeRelationId, acoerce->resulttype, 0));

                // the collation might not be referenced anywhere else, either
                if (acoerce->resultcollid != 0 && acoerce->resultcollid != DefaultCollationOid)
                {
                    addrs.Add(Address(CollationRelationId, acoerce->resultcollid, 0));
                }

                FindExprReferences((Node*)acoerce->arg, relId, addrs);
                FindExprReferences((Node*)acoerce->elemexpr, relId, addrs);
                return;
            }

            case NodeTag.T_ConvertRowtypeExpr:
            {
                ConvertRowtypeExpr* cvt = (ConvertRowtypeExpr*)node;

                // since there is no function dependency, need to depend on type
                addrs.Add(Address(TypeRelationId, cvt->resulttype, 0));
                FindExprReferences((Node*)cvt->arg, relId, addrs);
                return;
            }

            case NodeTag.T_CollateExpr:
            {
                CollateExpr* coll = (CollateExpr*)node;
                addrs.Add(Address(CollationRelationId, coll->collOid, 0));
                FindExprReferences((Node*)coll->arg, relId, addrs);
                return;
            }

            case NodeTag.T_RowExpr:
            {
                RowExpr* rowexpr = (RowExpr*)node;
                addrs.Add(Address(TypeRelationId, rowexpr->row_typeid, 0));
                FindListReferences(rowexpr->args, relId, addrs);
                return;
            }

            case NodeTag.T_RowCompareExpr:
            {
                RowCompareExpr* rcexpr = (RowCompareExpr*)node;
                for (int i = 0; rcexpr->opnos is not null && i < rcexpr->opnos->length; i++)
                {
                    addrs.Add(Address(OperatorRelationId, rcexpr->opnos->elements[i].oid_value, 0));
                }

                for (int i = 0; rcexpr->opfamilies is not null && i < rcexpr->opfamilies->length; i++)
                {
                    addrs.Add(Address(OperatorFamilyRelationId, rcexpr->opfamilies->elements[i].oid_value, 0));
                }

                FindListReferences(rcexpr->largs, relId, addrs);
                FindListReferences(rcexpr->rargs, relId, addrs);
                return;
            }

            case NodeTag.T_CoerceToDomain:
            {
                CoerceToDomain* cd = (CoerceToDomain*)node;
                addrs.Add(Address(TypeRelationId, cd->resulttype, 0));
                FindExprReferences((Node*)cd->arg, relId, addrs);
                return;
            }

            case NodeTag.T_NamedArgExpr:
                FindExprReferences((Node*)((NamedArgExpr*)node)->arg, relId, addrs);
                return;

            case NodeTag.T_BoolExpr:
                FindListReferences(((BoolExpr*)node)->args, relId, addrs);
                return;

            case NodeTag.T_CaseExpr:
            {
                CaseExpr* caseexpr = (CaseExpr*)node;
                FindExprReferences((Node*)caseexpr->arg, relId, addrs);
                FindListReferences(caseexpr->args, relId, addrs);
                FindExprReferences((Node*)caseexpr->defresult, relId, addrs);
                return;
            }

            case NodeTag.T_CaseWhen:
            {
                CaseWhen* when = (CaseWhen*)node;
                FindExprReferences((Node*)when->expr, relId, addrs);
                FindExprReferences((Node*)when->result, relId, addrs);
                return;
            }

            case NodeTag.T_ArrayExpr:
                FindListReferences(((ArrayExpr*)node)->elements, relId, addrs);
                return;

            case NodeTag.T_CoalesceExpr:
                FindListReferences(((CoalesceExpr*)node)->args, relId, addrs);
                return;

            case NodeTag.T_MinMaxExpr:
                FindListReferences(((MinMaxExpr*)node)->args, relId, addrs);
                return;

            case NodeTag.T_XmlExpr:
            {
                XmlExpr* xexpr = (XmlExpr*)node;
                FindListReferences(xexpr->named_args, relId, addrs);
                FindListReferences(xexpr->args, relId, addrs);
                return;
            }

            case NodeTag.T_NullTest:
                FindExprReferences((Node*)((NullTest*)node)->arg, relId, addrs);
                return;

            case NodeTag.T_BooleanTest:
                FindExprReferences((Node*)((BooleanTest*)node)->arg, relId, addrs);
                return;

            case NodeTag.T_SubscriptingRef:
            {
                SubscriptingRef* sbsref = (SubscriptingRef*)node;
                FindListReferences(sbsref->refupperindexpr, relId, addrs);
                FindListReferences(sbsref->reflowerindexpr, relId, addrs);
                FindExprReferences((Node*)sbsref->refexpr, relId, addrs);
                FindExprReferences((Node*)sbsref->refassgnexpr, relId, addrs);
                return;
            }

            case NodeTag.T_List:
                FindListReferences((List*)node, relId, addrs);
                return;

            case NodeTag.T_CaseTestExpr:
            case NodeTag.T_CoerceToDomainValue:
            case NodeTag.T_SetToDefault:
            case NodeTag.T_SQLValueFunction:
                return;

            default:
                throw new PgException(PgSqlStates.InternalError, $"unrecognized node type: {(int)node->type}");
        }
    }

    private static unsafe void FindListReferences(List* list, uint relId, List<ObjectAddress> addrs)
    {
        for (int i = 0; list is not null && i < list->length; i++)
        {
            FindExprReferences((Node*)list->elements[i].ptr_value, relId, addrs);
        }
    }

    private static void AddIfExists(string catalog, uint classId, uint objoid, List<ObjectAddress> addrs)
    {
        if (Spi.ExecuteScalar<bool>("SELECT EXISTS (SELECT FROM pg_catalog." + Spi.QuoteIdentifier(catalog) + " WHERE oid = $1)",
            SpiParameter.Create(objoid)))
        {
            addrs.Add(Address(classId, objoid, 0));
        }
    }

    /// <summary>
    /// Given an array of dependency references, eliminate any duplicates.
    /// </summary>
    private static void EliminateDuplicateDependencies(List<ObjectAddress> addrs)
    {
        if (addrs.Count <= 1)
        {
            return;    // nothing to do
        }

        // Sort the refs so that duplicates are adjacent; subId sorts unsigned so 0 comes first.
        addrs.Sort(static (obja, objb) =>
        {
            int cmp = obja.classId.CompareTo(objb.classId);
            if (cmp == 0)
            {
                cmp = obja.objectId.CompareTo(objb.objectId);
            }

            return cmp != 0 ? cmp : ((uint)obja.objectSubId).CompareTo((uint)objb.objectSubId);
        });

        // Remove dups
        int prior = 0;
        for (int oldref = 1; oldref < addrs.Count; oldref++)
        {
            ObjectAddress priorobj = addrs[prior];
            ObjectAddress thisobj = addrs[oldref];

            if (priorobj.classId == thisobj.classId && priorobj.objectId == thisobj.objectId)
            {
                if (priorobj.objectSubId == thisobj.objectSubId)
                {
                    continue;    // identical, so drop thisobj
                }

                // If we have a whole-object reference and a reference to a part of the same object, we don't need the
                // whole-object reference. The whole-object reference will always appear first in the sorted list.
                if (priorobj.objectSubId == 0)
                {
                    // replace whole ref with partial
                    priorobj.objectSubId = thisobj.objectSubId;
                    addrs[prior] = priorobj;
                    continue;
                }
            }

            // Not identical, so add thisobj to output set
            prior++;
            addrs[prior] = thisobj;
        }

        addrs.RemoveRange(prior + 1, addrs.Count - prior - 1);
    }
}
