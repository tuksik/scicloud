import os
from sqlalchemy import MetaData
from sqlalchemy import Column
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
    DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR


db = create_engine("postgresql://xiuli:Abcd1234@10.2.1.6/owncloud")

meta = MetaData(db)
oc_filecache = Table('oc_filecache', meta,
                    Column('fileid', INTEGER),
                    Column('storage', INTEGER),
                    Column('path', VARCHAR),
                    Column('path_hash', VARCHAR),
                    Column('parent', INTEGER),
                    Column('name', VARCHAR),
                    Column('mimetype', INTEGER),
                    Column('mimepart', INTEGER),
                    Column('size', BIGINT),
                    Column('mtime', INTEGER),
                    Column('storage_mtime', INTEGER),
                    Column('encrypted', INTEGER),
                    Column('unencrypted_size', BIGINT),
                    Column('etag', VARCHAR),
                    Column('permissions', INTEGER),
                    Column('checksum', VARCHAR)
                 )

oc_share = Table('oc_share', meta,
                   Column('id', INTEGER),
                   Column('share_type', SMALLINT),
                   Column('share_with', VARCHAR),
                   Column('uid_owner', VARCHAR),
                   Column('uid_initiator', VARCHAR),
                   Column('parent', INTEGER),
                   Column('item_type', VARCHAR),
                   Column('item_source', VARCHAR),
                   Column('item_target', VARCHAR),
                   Column('file_source', INTEGER),
                   Column('file_target', VARCHAR),
                   Column('permissions', SMALLINT), # 15: edit (5:create, 3:change, 9:delete) 17:share, 31: share+edit, 25: share+ edit-delete, 19: share+edit-change
                   Column('stime', BIGINT),
                   Column('accepted', SMALLINT),
                   Column('expiration', TIMESTAMP),
                   Column('token', VARCHAR),
                   Column('mail_send', SMALLINT)
                   )

oc_group_user = Table('oc_group_user', meta,
                   Column('gid', VARCHAR),
                   Column('uid', VARCHAR)
                   )

with db.connect() as conn:
    join_share_group_user_filecache = oc_share.join(oc_group_user, oc_share.c.share_with==oc_group_user.c.gid)\
        .join(oc_filecache, oc_share.c.file_source==oc_filecache.c.fileid)

    stmt = select([oc_share.c.uid_owner, oc_group_user.c.uid, oc_filecache.c.path, oc_share.c.item_type, oc_share.c.permissions])\
        .select_from(join_share_group_user_filecache)\
        .where(oc_share.c.share_type == 1)
    results = conn.execute(stmt).fetchall()

    join_share_filecache = oc_share.join(oc_filecache, oc_share.c.file_source == oc_filecache.c.fileid)
    stmt = select([oc_share.c.uid_owner, oc_share.c.share_with, oc_filecache.c.path, oc_share.c.item_type, oc_share.c.permissions])\
        .select_from(join_share_filecache)\
        .where(oc_share.c.share_type == 0)
    results.extend(conn.execute(stmt).fetchall())

    links = []
    existing_links = set()
    for (owner, sharewith, oc_path, item_type, permissions) in results:
        if not 'trashbin' in oc_path:
            local_path = oc_path.lstrip('files') #'files/data/Sonderborg' ->  '/data/Sonderborg'
            idx = local_path.rfind('/')
            target_path = local_path[:idx] #'/data'
            item_name = local_path[idx:] # '/Sonderborg'

            link = "/home/{}{}".format(sharewith, local_path)
            links.append(link)

            path = "/home/{}{}".format(sharewith, target_path)
            if target_path:
                os.system("mkdir -p {}".format(path))

            for name in os.listdir(path):
                l = "{}/{}".format(path, name)
                if os.path.islink(l) and not 'share' in l:
                    existing_links.add(l)

            if not os.path.exists(link):
                os.system("ln -s /opt/owncloud{} {}".format(local_path, link))

    for existing_link in existing_links: # reovke the shared
        if not existing_link in links:
            os.system("unlink {}".format(existing_link))

