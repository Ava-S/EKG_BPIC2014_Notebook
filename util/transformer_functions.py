# Import logging and surpress warnings
import logging

logging.getLogger("neo4j").setLevel(logging.ERROR)
logging.getLogger("pd").setLevel(logging.ERROR)

# Import promg
from promg import Query


def index_exists(_db_connection, index_name):
    query = '''
        show INDEX 
        YIELD name
        RETURN collect(distinct name) as index_names
    '''

    result = _db_connection.exec_query(query)
    index_names = result[0]['index_names']
    return index_name in index_names


def create_event_timestamp_index(_db_connection, _label='Event', _timestamp_field='timestamp'):
    index_name = f'{_label.lower()}_{_timestamp_field.lower()}_index'
    if not index_exists(_db_connection, index_name):
        index_query_str = """
                          CREATE INDEX $index_name IF NOT EXISTS
                              FOR (e:$label)
                              ON (e.$timestamp_field) \
                          """

        index_query = Query(query_str=index_query_str,
                            template_string_parameters={
                                "label": _label,
                                "timestamp_field": _timestamp_field
                            },
                            parameters={"index_name": index_name})

        _db_connection.exec_query(index_query)
        print(f"→ Index for :{_label} ({_timestamp_field}) created to improve performance")


def create_index(_db_connection, _label):
    index_name = f"{_label.lower()}_sysId_index"

    if not index_exists(_db_connection, index_name):
        index_query_str = f"""
            CREATE INDEX $index_name IF NOT EXISTS
            FOR (n:$label)
            ON (n.sysId)
        """

        index_query = Query(query_str=index_query_str,
                            parameters={
                                "index_name": index_name
                            },
                            template_string_parameters={
                                "label": _label
                            })

        _db_connection.exec_query(index_query)
        print(f"→ Index for :{_label}(sysId) created to improve performance")


def build_entity(_db_connection, _label, _config):
    iterate_query = """
        :auto
        MATCH (l:Log)-[:CONTAINS]->(r:Record)
        WHERE r.$sysId_field IS NOT NULL $log_name_condition $time_field_condition
        WITH r.$sysId_field $id_addition AS sysId, r
        CALL (sysId, r) {
             MERGE (n:$label {sysId: sysId})
             MERGE (n)-[:EXTRACTED_FROM]->(r)
             $attr_updates
             $constants_updates
        } IN TRANSACTIONS
    """
    attr_updates = ""
    time_field_condition = ""

    if "attributes" in _config:
        attr_updates += "SET "
        attr_updates += ", ".join(
            [f"n.{key} = COALESCE(n.{key}, r.{attr})" for key, attr in _config["attributes"].items()])

        if "timestamp" in _config["attributes"]:
            time_field_condition = f"AND r.{_config['attributes']['timestamp']} IS NOT NULL"

    constants_updates = ""
    if "constants" in _config:
        constants_updates += "SET "
        constants_updates += ", ".join(
            [f"n.{key} = COALESCE(n.{key}, {attr})" for key, attr in _config["constants"].items()])

    query = Query(
        query_str=iterate_query,
        parameters={
            "log_name": _config["log"],
        },
        template_string_parameters={
            "label": _label,
            "sysId_field": _config["sysId"],
            "log_name_condition": "AND l.name = $log_name" if _config["log"] else "",
            "time_field_condition": time_field_condition,
            "attr_updates": attr_updates,
            "constants_updates": constants_updates,
            "id_addition": f"+ '{_config['id_addition']}'" if 'id_addition' in _config else ""
        }
    )
    _db_connection.exec_query(query)
    print(f"→ {_label} nodes created.")


def build_entities(_db_connection, entities):
    """
    Create entities. Includes indexing.
    """
    print("\n=== INDEXES ===")
    for _label in entities.keys():
        try:
            create_index(_db_connection=_db_connection,
                         _label=_label)
        except Exception as e:
            print(f"Failed to create index for {_label}: {e}")

    print(f"\n=== Building ENTITY NODES ===")

    for _label, _configs in entities.items():
        for _config in _configs:
            try:
                build_entity(_db_connection=_db_connection,
                             _label=_label,
                             _config=_config)
            except Exception as e:
                print(f"Failed for {_label}: {e}")


def build_foreign_key_index(_db_connection, _config):
    foreign_key_query_str = '''
                            CREATE INDEX $index_name IF NOT EXISTS
                                FOR (n:Record) ON (n.$foreign_key) \
                            '''

    for _type in ["from_object", "to_object"]:
        if "foreign_key" in _config[_type]:
            foreign_key = _config[_type]["foreign_key"]

            foreign_key_index_query = Query(
                query_str=foreign_key_query_str,
                parameters={
                    "index_name": f"record_{foreign_key}_index"
                },
                template_string_parameters={
                    "foreign_key": foreign_key
                }
            )

            _db_connection.exec_query(foreign_key_index_query)
            print(f"Index ensured for :Record({foreign_key})")


def build_relationship(_db_connection, _type, _config):
    o2o_query_str = '''
        :auto
         MATCH (from:$from_object) - [:EXTRACTED_FROM] -> (r:Record) <- [:EXTRACTED_FROM] - (to:$to_object)
         $log_condition
         WHERE $condition
         CALL (from, to, r) {
            MERGE (from) - [rel:$type] -> (to)
            $attr_updates
            $constants_updates
        } IN TRANSACTIONS
    '''

    attr_updates = ""
    if "attributes" in _config:
        attr_updates = "SET "
        attr_updates += ", ".join(
            [f"rel.{key} = COALESCE(rel.{key}, r.{attr})" for key, attr in _config["attributes"].items()])
    constants_updates = ""
    if "constants" in _config:
        constants_updates += "SET "
        constants_updates += ", ".join(
            [f"rel.{key} = COALESCE(rel.{key}, {attr})" for key, attr in _config["constants"].items()])

    from_object = _config["from_object"]
    to_object = _config["to_object"]

    from_foreign_key = ""
    to_foreign_key = ""
    log = ""
    conditions = []
    log_condition = ""

    if "foreign_key" in from_object:
        from_foreign_key = from_object["foreign_key"]
        conditions.append("r[$from_foreign_key] IS NOT NULL AND from.sysId = r[$from_foreign_key]")
    if "foreign_key" in to_object:
        to_foreign_key = to_object["foreign_key"]
        conditions.append("r[$to_foreign_key] IS NOT NULL AND to.sysId = r[$to_foreign_key]")
    if "log" in _config:
        log = _config["log"]
        log_condition = "MATCH (r) <- [:CONTAINS] - (:Log {name: $log_name})"

    o2o_query = Query(
        query_str=o2o_query_str,
        parameters={
            "from_foreign_key": from_foreign_key,
            "to_foreign_key": to_foreign_key,
            "log_name": log
        },

        template_string_parameters={
            "condition": " AND ".join(conditions),
            "from_object": from_object["label"],
            "to_object": to_object["label"],
            "type": _type,
            "attr_updates": attr_updates,
            "constants_updates": constants_updates,
            "log_condition": log_condition
        }
    )

    _db_connection.exec_query(o2o_query)
    print(f"→ (:{_config['from_object']}) - [:{_type}] -> (:{_config['to_object']}) Relationship built")


def build_relationships(_db_connection, _relationships):
    print("\n=== INDEXES ===")
    for _type, _configs in _relationships.items():
        for _config in _configs:
            build_foreign_key_index(_db_connection=_db_connection,
                                    _config=_config)

    print("\n=== O2O RELATIONSHIPS ===")
    for _type, _configs in _relationships.items():
        for _config in _configs:
            build_relationship(_db_connection=_db_connection,
                               _type=_type,
                               _config=_config)
