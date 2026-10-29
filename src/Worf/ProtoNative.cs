using System.Runtime.CompilerServices;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf binary protocol functions.
/// </summary>
internal static unsafe class ProtoNative
{
    private const byte IsReplicaIdentity = 1;

    // An ORIGIN message's flag for the id of the node that wrote the transaction, which follows the origin's name.
    private const byte OriginFlagNodeId = 1;

    // FirstNormalObjectId from access/transam.h.
    private const uint FirstNormalObjectId = 16384;

    /// <summary>
    /// Write relation description to the output stream.
    /// </summary>
    internal static void WriteRel(StringInfoData* @out, WorfOutputData data, RelationData* rel, HashSet<int>? attList)
    {
        _ = data;
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'R');      // sending RELATION

        // send the flags field
        NativeMethods.pq_sendbyte(@out, flags);

        // use Oid as relation identifier
        NativeMethods.pq_sendint32(@out, rel->rd_id);

        sbyte* nspname = NativeMethods.get_namespace_name(rel->rd_rel->relnamespace);
        if (nspname is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"cache lookup failed for namespace {rel->rd_rel->relnamespace}");
        }

        byte nspnamelen = (byte)(Worf.StrLen(nspname) + 1);

        sbyte* relname = (sbyte*)&rel->rd_rel->relname;
        byte relnamelen = (byte)(Worf.StrLen(relname) + 1);

        NativeMethods.pq_sendbyte(@out, nspnamelen);      // schema name length
        Worf.PqSendBytes(@out, nspname, nspnamelen);

        NativeMethods.pq_sendbyte(@out, relnamelen);      // table name length
        Worf.PqSendBytes(@out, relname, relnamelen);

        // send the attribute info
        WriteAttrs(@out, rel, attList);

        NativeMethods.pfree(nspname);
    }

    /// <summary>
    /// Write BEGIN to the output stream.
    /// </summary>
    internal static void WriteBegin(StringInfoData* @out, WorfOutputData data, ReorderBufferTXN* txn)
    {
        _ = data;
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'B');      // BEGIN

        // send the flags field its self
        NativeMethods.pq_sendbyte(@out, flags);

        // fixed fields
        NativeMethods.pq_sendint64(@out, txn->final_lsn);
        NativeMethods.pq_sendint64(@out, unchecked((ulong)CommitTime(txn)));
        NativeMethods.pq_sendint32(@out, txn->xid);
    }

    /// <summary>
    /// Write COMMIT to the output stream.
    /// </summary>
    internal static void WriteCommit(StringInfoData* @out, WorfOutputData data, ReorderBufferTXN* txn, ulong commitLsn)
    {
        _ = data;
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'C');      // sending COMMIT

        // send the flags field
        NativeMethods.pq_sendbyte(@out, flags);

        // send fixed fields
        NativeMethods.pq_sendint64(@out, commitLsn);
        NativeMethods.pq_sendint64(@out, txn->end_lsn);
        NativeMethods.pq_sendint64(@out, unchecked((ulong)CommitTime(txn)));
    }

    /// <summary>
    /// Write ORIGIN to the output stream.
    /// </summary>
    /// <param name="out">The output.</param>
    /// <param name="origin">The origin's name.</param>
    /// <param name="originLsn">The transaction's position at its origin.</param>
    /// <param name="nodeId">The node that wrote the transaction, or zero when it isn't known.</param>
    internal static void WriteOrigin(StringInfoData* @out, sbyte* origin, ulong originLsn, uint nodeId)
    {
        byte flags = nodeId != 0 ? OriginFlagNodeId : (byte)0;

        NativeMethods.pq_sendbyte(@out, (byte)'O');      // ORIGIN

        // send the flags field its self
        NativeMethods.pq_sendbyte(@out, flags);

        // fixed fields
        NativeMethods.pq_sendint64(@out, originLsn);

        // origin
        byte len = (byte)(Worf.StrLen(origin) + 1);
        NativeMethods.pq_sendbyte(@out, len);
        Worf.PqSendBytes(@out, origin, len);

        if (nodeId != 0)
        {
            NativeMethods.pq_sendint32(@out, nodeId);
        }
    }

    /// <summary>
    /// Write INSERT to the output stream.
    /// </summary>
    internal static void WriteInsert(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* newtuple,
        HashSet<int>? attList)
    {
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'I');      // action INSERT

        // send the flags field
        NativeMethods.pq_sendbyte(@out, flags);

        // use Oid as relation identifier
        NativeMethods.pq_sendint32(@out, rel->rd_id);

        NativeMethods.pq_sendbyte(@out, (byte)'N');      // new tuple follows
        WriteTuple(@out, data, rel, newtuple, attList);
    }

    /// <summary>
    /// Write UPDATE to the output stream.
    /// </summary>
    internal static void WriteUpdate(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* oldtuple,
        HeapTupleData* newtuple, HashSet<int>? attList)
    {
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'U');      // action UPDATE

        // send the flags field
        NativeMethods.pq_sendbyte(@out, flags);

        // use Oid as relation identifier
        NativeMethods.pq_sendint32(@out, rel->rd_id);

        // TODO: support whole tuple (O tuple type)
        //
        // Right now we can only write the key-part since logical decoding doesn't know how to record the whole old
        // tuple for us in WAL. We can't use REPLICA IDENTITY FULL for this, since that makes the key-part the whole
        // tuple, causing issues with conflict resultion and index lookups. We need a separate decoding option to
        // record whole tuples.
        if (oldtuple is not null)
        {
            NativeMethods.pq_sendbyte(@out, (byte)'K');  // old key follows
            WriteTuple(@out, data, rel, oldtuple, attList);
        }

        NativeMethods.pq_sendbyte(@out, (byte)'N');      // new tuple follows
        WriteTuple(@out, data, rel, newtuple, attList);
    }

    /// <summary>
    /// Write DELETE to the output stream.
    /// </summary>
    internal static void WriteDelete(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* oldtuple,
        HashSet<int>? attList)
    {
        byte flags = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'D');      // action DELETE

        // send the flags field
        NativeMethods.pq_sendbyte(@out, flags);

        // use Oid as relation identifier
        NativeMethods.pq_sendint32(@out, rel->rd_id);

        // TODO support whole tuple ('O' tuple type)
        //
        // See notes on update for details
        NativeMethods.pq_sendbyte(@out, (byte)'K');      // old key follows
        WriteTuple(@out, data, rel, oldtuple, attList);
    }

    /// <summary>
    /// Most of the brains for startup message creation lives in <see cref="OutputConfig"/>, so this presently just
    /// sends the set of key/value pairs.
    /// </summary>
    internal static void WriteStartupMessage(StringInfoData* @out, List<(string Key, string Value)> msg)
    {
        NativeMethods.pq_sendbyte(@out, (byte)'S');      // message type field
        NativeMethods.pq_sendbyte(@out, OutputProto.StartupMsgFormatFlat);  // startup message version
        foreach ((string key, string value) in msg)
        {
            // null-terminated key and value pairs, in client_encoding
            NativeMethods.pq_sendstring(@out, Worf.PStrDup(key));
            NativeMethods.pq_sendstring(@out, Worf.PStrDup(value));
        }
    }

    /// <summary>
    /// Read transaction BEGIN from the stream.
    /// </summary>
    internal static void ReadBegin(StringInfoData* @in, out ulong remoteLsn, out long committime, out uint remoteXid)
    {
        // read flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        // read fields
        remoteLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(@in));
        committime = NativeMethods.pq_getmsgint64(@in);
        remoteXid = NativeMethods.pq_getmsgint(@in, 4);
    }

    /// <summary>
    /// Read transaction COMMIT from the stream.
    /// </summary>
    internal static void ReadCommit(StringInfoData* @in, out ulong commitLsn, out ulong endLsn, out long committime)
    {
        // read flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        // read fields
        commitLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(@in));
        endLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(@in));
        committime = NativeMethods.pq_getmsgint64(@in);
    }

    /// <summary>
    /// Read ORIGIN from the output stream.
    /// </summary>
    /// <param name="in">The input.</param>
    /// <param name="originLsn">The transaction's position at its origin.</param>
    /// <param name="nodeId">The node that wrote the transaction, or zero when the provider didn't say.</param>
    /// <returns>The origin's name.</returns>
    internal static string ReadOrigin(StringInfoData* @in, out ulong originLsn, out uint nodeId)
    {
        // read the flags
        int flags = NativeMethods.pq_getmsgbyte(@in);

        // fixed fields
        originLsn = unchecked((ulong)NativeMethods.pq_getmsgint64(@in));

        // origin
        int len = NativeMethods.pq_getmsgbyte(@in);
        sbyte* origin = NativeMethods.pq_getmsgbytes(@in, len);
        int strlen = 0;
        while (strlen < len && origin[strlen] != 0)
        {
            strlen++;
        }

        nodeId = (flags & OriginFlagNodeId) != 0 ? NativeMethods.pq_getmsgint(@in, 4) : 0;

        return Worf.FromServer(new ReadOnlySpan<byte>(origin, strlen));
    }

    /// <summary>
    /// Read INSERT from stream. Fills the new tuple.
    /// </summary>
    internal static WorfRelation ReadInsert(StringInfoData* @in, PgLockMode lockmode, out WorfTupleData newtup)
    {
        // read the flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        // read the relation id
        uint relid = NativeMethods.pq_getmsgint(@in, 4);

        int action = NativeMethods.pq_getmsgbyte(@in);
        if (action != 'N')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected new tuple but got {action}");
        }

        WorfRelation rel = RelCache.RelationOpen(relid, lockmode);

        newtup = ReadTuple(@in, rel);

        return rel;
    }

    /// <summary>
    /// Read UPDATE from stream.
    /// </summary>
    internal static WorfRelation ReadUpdate(StringInfoData* @in, PgLockMode lockmode, out bool hasoldtup,
        out WorfTupleData? oldtup, out WorfTupleData newtup)
    {
        // read the flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        // read the relation id
        uint relid = NativeMethods.pq_getmsgint(@in, 4);

        // read and verify action
        int action = NativeMethods.pq_getmsgbyte(@in);
        if (action != 'K' && action != 'O' && action != 'N')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected action 'N', 'O' or 'K', got {(char)action}");
        }

        WorfRelation rel = RelCache.RelationOpen(relid, lockmode);

        // check for old tuple
        if (action is 'K' or 'O')
        {
            oldtup = ReadTuple(@in, rel);
            hasoldtup = true;
            action = NativeMethods.pq_getmsgbyte(@in);
        }
        else
        {
            oldtup = null;
            hasoldtup = false;
        }

        // check for new  tuple
        if (action != 'N')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected action 'N', got {(char)action}");
        }

        newtup = ReadTuple(@in, rel);

        return rel;
    }

    /// <summary>
    /// Read DELETE from stream. Fills the old tuple.
    /// </summary>
    internal static WorfRelation ReadDelete(StringInfoData* @in, PgLockMode lockmode, out WorfTupleData oldtup)
    {
        // read the flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        // read the relation id
        uint relid = NativeMethods.pq_getmsgint(@in, 4);

        // read and verify action
        int action = NativeMethods.pq_getmsgbyte(@in);
        if (action != 'K' && action != 'O')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected action 'O' or 'K' {(char)action}");
        }

        WorfRelation rel = RelCache.RelationOpen(relid, lockmode);

        oldtup = ReadTuple(@in, rel);

        return rel;
    }

    /// <summary>
    /// Read schema.relation from stream and record it in the relation cache.
    /// </summary>
    internal static uint ReadRel(StringInfoData* @in)
    {
        // read the flags
        _ = NativeMethods.pq_getmsgbyte(@in);

        uint relid = NativeMethods.pq_getmsgint(@in, 4);

        // Read relation from stream
        int len = NativeMethods.pq_getmsgbyte(@in);
        string schemaname = CString(NativeMethods.pq_getmsgbytes(@in, len), len);

        len = NativeMethods.pq_getmsgbyte(@in);
        string relname = CString(NativeMethods.pq_getmsgbytes(@in, len), len);

        // Get attribute description
        string[] attrnames = ReadAttrs(@in);

        RelCache.RelationCacheUpdate(relid, schemaname, relname, attrnames);

        return relid;
    }

    /// <summary>
    /// Write relation attributes to the outputstream.
    /// </summary>
    private static void WriteAttrs(StringInfoData* @out, RelationData* rel, HashSet<int>? attList)
    {
        TupleDescData* desc = rel->rd_att;
        ushort nliveatts = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'A');      // sending ATTRS

        // send number of live attributes
        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);

            if (att->attisdropped)
            {
                continue;
            }

            if (attList is not null && !attList.Contains(att->attnum))
            {
                continue;
            }

            nliveatts++;
        }

        NativeMethods.pq_sendint16(@out, nliveatts);

        // fetch bitmap of REPLICATION IDENTITY attributes
        Bitmapset* idattrs = NativeMethods.RelationGetIndexAttrBitmap(rel, IndexAttrBitmapKind.INDEX_ATTR_BITMAP_IDENTITY_KEY);

        // send the attributes
        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);
            byte flags = 0;

            if (att->attisdropped)
            {
                continue;
            }

            if (attList is not null && !attList.Contains(att->attnum))
            {
                continue;
            }

            if (NativeMethods.bms_is_member(att->attnum - ReplicationSet.FirstLowInvalidHeapAttributeNumber, idattrs))
            {
                flags |= IsReplicaIdentity;
            }

            NativeMethods.pq_sendbyte(@out, (byte)'C');  // column definition follows
            NativeMethods.pq_sendbyte(@out, flags);

            NativeMethods.pq_sendbyte(@out, (byte)'N');  // column name block follows
            sbyte* attname = (sbyte*)&att->attname;
            ushort len = (ushort)(Worf.StrLen(attname) + 1);
            NativeMethods.pq_sendint16(@out, len);
            Worf.PqSendBytes(@out, attname, len);  // data
        }

        NativeMethods.bms_free(idattrs);
    }

    /// <summary>
    /// Write a tuple to the outputstream, in the most efficient format possible.
    /// </summary>
    private static void WriteTuple(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* tuple,
        HashSet<int>? attList)
    {
        TupleDescData* desc = rel->rd_att;
        ushort nliveatts = 0;

        NativeMethods.pq_sendbyte(@out, (byte)'T');      // sending TUPLE

        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);

            if (att->attisdropped)
            {
                continue;
            }

            if (attList is not null && !attList.Contains(att->attnum))
            {
                continue;
            }

            nliveatts++;
        }

        NativeMethods.pq_sendint16(@out, nliveatts);

        // try to allocate enough memory from the get go
        NativeMethods.enlargeStringInfo(@out, (int)tuple->t_len + nliveatts * (1 + 4));

        ulong* values = (ulong*)NativeMethods.palloc((ulong)(desc->natts * sizeof(ulong)));
        bool* isnull = (bool*)NativeMethods.palloc((ulong)desc->natts);
        NativeMethods.heap_deform_tuple(tuple, desc, values, isnull);

        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);

            // skip dropped columns
            if (att->attisdropped)
            {
                continue;
            }

            if (attList is not null && !attList.Contains(att->attnum))
            {
                continue;
            }

            if (isnull[i])
            {
                NativeMethods.pq_sendbyte(@out, (byte)'n');  // null column
                continue;
            }
            else if (att->attlen == -1 && Varlena.IsExternalOndisk(values[i]))
            {
                NativeMethods.pq_sendbyte(@out, (byte)'u');  // unchanged toast column
                continue;
            }

            HeapTupleData* typtup = NativeMethods.typeidType(att->atttypid);
            FormData_pg_type* typclass = (FormData_pg_type*)NativeMethods.GETSTRUCT(typtup);
            uint typsend = typclass->typsend;
            uint typoutput = typclass->typoutput;

            char transferType = DecideDatumTransfer(att, typclass, data.AllowInternalBasetypes, data.AllowBinaryBasetypes);
            NativeMethods.ReleaseSysCache(typtup);

            switch (transferType)
            {
                case 'i':
                    NativeMethods.pq_sendbyte(@out, (byte)'i');  // internal-format binary data follows

                    // pass by value
                    if (att->attbyval)
                    {
                        NativeMethods.pq_sendint32(@out, (uint)att->attlen);  // length

                        ulong value = values[i];
                        Worf.AppendBinaryStringInfo(@out, &value, att->attlen);
                    }

                    // fixed length non-varlena pass-by-reference type
                    else if (att->attlen > 0)
                    {
                        NativeMethods.pq_sendint32(@out, (uint)att->attlen);  // length

                        Worf.AppendBinaryStringInfo(@out, (void*)values[i], att->attlen);
                    }

                    // varlena type
                    else if (att->attlen == -1)
                    {
                        byte* ptr = (byte*)values[i];

                        // send indirect datums inline
                        if (Varlena.IsExternalIndirect(values[i]))
                        {
                            ptr = (byte*)Unsafe.ReadUnaligned<nint>(ptr + 2);
                        }

                        int size = Varlena.SizeAny(ptr);
                        NativeMethods.pq_sendint32(@out, (uint)size);  // length

                        Worf.AppendBinaryStringInfo(@out, ptr, size);
                    }
                    else
                    {
                        throw new PgException(PgSqlStates.InternalError, "unsupported tuple type");
                    }

                    break;

                case 'b':
                {
                    NativeMethods.pq_sendbyte(@out, (byte)'b');  // binary send/recv data follows

                    varlena* outputbytes = NativeMethods.OidSendFunctionCall(typsend, values[i]);

                    int len = Varlena.Size((byte*)outputbytes) - Varlena.Hdrsz;
                    NativeMethods.pq_sendint32(@out, (uint)len);  // length
                    Worf.PqSendBytes(@out, (byte*)outputbytes + Varlena.Hdrsz, len);  // data
                    NativeMethods.pfree(outputbytes);
                    break;
                }

                default:
                {
                    NativeMethods.pq_sendbyte(@out, (byte)'t');  // 'text' data follows

                    sbyte* outputstr = NativeMethods.OidOutputFunctionCall(typoutput, values[i]);
                    int len = Worf.StrLen(outputstr) + 1;
                    NativeMethods.pq_sendint32(@out, (uint)len);  // length
                    Worf.AppendBinaryStringInfo(@out, outputstr, len);  // data
                    NativeMethods.pfree(outputstr);
                    break;
                }
            }
        }

        NativeMethods.pfree(values);
        NativeMethods.pfree(isnull);
    }

    /// <summary>
    /// Make the executive decision about which protocol to use.
    /// </summary>
    private static char DecideDatumTransfer(FormData_pg_attribute* att, FormData_pg_type* typclass,
        bool allowInternalBasetypes, bool allowBinaryBasetypes)
    {
        // Use the binary protocol, if allowed, for builtin & plain datatypes.
        if (allowInternalBasetypes &&
            typclass->typtype == 'b' &&
            att->atttypid < FirstNormalObjectId &&
            typclass->typelem == 0)
        {
            return 'i';
        }

        // Use send/recv, if allowed, if the type is plain or builtin.
        //
        // XXX: we can't use send/recv for array or composite types for now due to the embedded oids.
        else if (allowBinaryBasetypes &&
                 typclass->typreceive != 0 &&
                 (att->atttypid < FirstNormalObjectId || typclass->typtype != 'c') &&
                 (att->atttypid < FirstNormalObjectId || typclass->typelem == 0))
        {
            return 'b';
        }

        return 't';
    }

    /// <summary>
    /// Read tuple in remote format from stream. The returned tuple is converted to the local relation tuple format.
    /// </summary>
    private static WorfTupleData ReadTuple(StringInfoData* @in, WorfRelation rel)
    {
        int action = NativeMethods.pq_getmsgbyte(@in);
        if (action != 'T')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected TUPLE, got {(char)action}");
        }

        TupleDescData* desc = (rel.Rel)->rd_att;
        WorfTupleData tuple = new(desc->natts);

        int natts = (int)NativeMethods.pq_getmsgint(@in, 2);
        if (rel.Natts != natts)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"tuple natts mismatch between remote relation metadata cache (natts={rel.Natts}) and remote tuple data (natts={natts})");
        }

        // Read the data
        for (int i = 0; i < natts; i++)
        {
            int attid = rel.Attmap[i];
            char kind = (char)NativeMethods.pq_getmsgbyte(@in);
            int len;

            // A column dropped here is read past.
            if (attid < 0)
            {
                switch (kind)
                {
                    case 'n' or 'u':
                        break;

                    case 'i' or 'b' or 't':
                        _ = NativeMethods.pq_getmsgbytes(@in, (int)NativeMethods.pq_getmsgint(@in, 4));
                        break;

                    default:
                        throw new PgException(PgSqlStates.InternalError, $"unknown data representation type '{kind}'");
                }

                continue;
            }

            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, attid);

            tuple.Kinds[attid] = kind;
            switch (kind)
            {
                case 'n': // null
                    // already marked as null
                    tuple.Values[attid] = 0xdeadbeef;
                    tuple.Changed[attid] = true;
                    tuple.Converted[attid] = true;
                    break;

                case 'u': // unchanged column
                    tuple.Values[attid] = 0xfbadbeef; // make bad usage more obvious
                    tuple.Converted[attid] = true;
                    break;

                case 'i': // internal binary format
                {
                    tuple.Nulls[attid] = false;
                    tuple.Changed[attid] = true;

                    len = (int)NativeMethods.pq_getmsgint(@in, 4); // read length
                    sbyte* data = NativeMethods.pq_getmsgbytes(@in, len);

                    // and data
                    tuple.Values[attid] = att->attbyval ? FetchAttByval(data, len) : (ulong)data;
                    tuple.Converted[attid] = true;
                    break;
                }

                case 'b': // binary send/recv format
                case 't': // text format
                {
                    tuple.Nulls[attid] = false;
                    tuple.Changed[attid] = true;

                    len = (int)NativeMethods.pq_getmsgint(@in, 4); // read length
                    sbyte* data = NativeMethods.pq_getmsgbytes(@in, len);

                    // Text arrives with its terminating zero.
                    int datalen = kind == 't' ? Math.Max(len - 1, 0) : len;
                    tuple.Data[attid] = new ReadOnlySpan<byte>(data, datalen).ToArray();
                    break;
                }

                default:
                    throw new PgException(PgSqlStates.InternalError, $"unknown data representation type '{kind}'");
            }
        }

        return tuple;
    }

    /// <summary>
    /// Read relation attributes from the outputstream.
    /// </summary>
    private static string[] ReadAttrs(StringInfoData* @in)
    {
        int blocktype = NativeMethods.pq_getmsgbyte(@in);
        if (blocktype != 'A')
        {
            throw new PgException(PgSqlStates.InternalError, $"expected ATTRS, got {(char)blocktype}");
        }

        int nattrs = (int)NativeMethods.pq_getmsgint(@in, 2);
        string[] attrs = new string[nattrs];

        // read the attributes
        for (int i = 0; i < nattrs; i++)
        {
            blocktype = NativeMethods.pq_getmsgbyte(@in);      // column definition follows
            if (blocktype != 'C')
            {
                throw new PgException(PgSqlStates.InternalError, $"expected COLUMN, got {(char)blocktype}");
            }

            // read flags (we ignore them so far)
            _ = NativeMethods.pq_getmsgbyte(@in);

            blocktype = NativeMethods.pq_getmsgbyte(@in);      // column name block follows
            if (blocktype != 'N')
            {
                throw new PgException(PgSqlStates.InternalError, $"expected NAME, got {(char)blocktype}");
            }

            // attribute name
            int len = (int)NativeMethods.pq_getmsgint(@in, 2);

            // the string is NULL terminated
            attrs[i] = CString(NativeMethods.pq_getmsgbytes(@in, len), len);
        }

        return attrs;
    }

    private static string CString(sbyte* data, int maxlen)
    {
        int len = 0;
        while (len < maxlen && data[len] != 0)
        {
            len++;
        }

        return Worf.FromServer(new ReadOnlySpan<byte>(data, len));
    }

    private static ulong FetchAttByval(sbyte* data, int len) => len switch
    {
        1 => unchecked((ulong)*data),
        2 => unchecked((ulong)Unsafe.ReadUnaligned<short>(data)),
        4 => unchecked((ulong)Unsafe.ReadUnaligned<int>(data)),
        8 => Unsafe.ReadUnaligned<ulong>(data),
        _ => throw new PgException(PgSqlStates.InternalError, "unsupported byval length: " + len),
    };

    private static long CommitTime(ReorderBufferTXN* txn)
    {
#if ANKUS_PG19
        return txn->commit_time;
#else
        return txn->xact_time.commit_time;
#endif
    }
}

