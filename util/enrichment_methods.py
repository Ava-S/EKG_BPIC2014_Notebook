# Import logging and surpress warnings
import logging

from util.assign_types_functions import add_object_type_node
from util.transformer_functions import create_index, create_event_timestamp_index

logging.getLogger("neo4j").setLevel(logging.ERROR)
logging.getLogger("pd").setLevel(logging.ERROR)

# Import promg
from promg import Query


#######################################################################
##################### ENRICHMENT METHOD 1 #############################
##################### MATERIALIZE OBJECTS #############################
#######################################################################

def materialize_object(_db_connection, _label, _config):
    from_object = _config["from_object"]
    to_object = _config["to_object"]
    set_attributes = []

    for object_type, _object in {"from": from_object, "to": to_object}.items():
        if "attributes" in _object:
            set_attributes.extend(
                [f"new.{key} = COALESCE(new.{key}, {object_type}.{attr})" for key, attr in
                 _object["attributes"].items()])

    materialize_relationship_query = '''
        :auto
        MATCH (from) - [ :IS_OF_TYPE] -> (:ObjectType {objectType: $from_object})
        MATCH (to) - [ :IS_OF_TYPE] -> (:ObjectType {objectType: $to_object})
        MATCH (from) - [r WHERE type(r) = $relation_type] -> (to)
        CALL (from, r, to) {
            MERGE (new:$materialized_object {sysId: from.sysId + '_' + to.sysId})
            MERGE (from) <- [:RELATED] - (new) - [:RELATED] -> (to)
            SET new[$from_object] = from.sysId,
                new[$to_object] = to.sysId
            $set_attributes
        } IN TRANSACTIONS
        RETURN count(r) as count
    '''

    materialize_query = Query(
        query_str=materialize_relationship_query,
        parameters={
            "from_object": from_object["label"],
            "to_object": to_object["label"],
            "relation_type": _config["relation_type"]
        },
        template_string_parameters={
            "materialized_object": _label,
            "set_attributes": "SET " + ", ".join(set_attributes)
        }
    )

    result = _db_connection.exec_query(materialize_query)
    print(f"→ {result[0]['count']} {_label} nodes created.")


def materialize_objects(_db_connection, _objects_to_materialize):
    """
    Create entities. Includes indexing.
    """

    print("\n=== Materializing Relationships into Objects ===")
    for _label, _configs in _objects_to_materialize.items():
        for _config in _configs:
            try:
                create_index(_db_connection=_db_connection,
                             _label=_label)
            except Exception as e:
                print(f"Failed to create index for {_label}: {e}")
                return

            try:
                materialize_object(
                    _db_connection=_db_connection,
                    _label=_label,
                    _config=_config)

                add_object_type_node(
                    _db_connection=_db_connection,
                    _object_type=_label
                )

            except Exception as e:
                print(f"Failed to materialize object {_label}: {e}")


#######################################################################
####################### ENRICHMENT METHOD 2 + 3 #######################
########### Infer E2O and O2O relationships in simple cases ###########
#######################################################################

def extend_relationship(_db_connection, _type, _config):
    from_object = _config["from_object"]
    to_object = _config["to_object"]

    query_str = '''
        :auto
        MATCH (from:$from_object)
        MATCH (to:$to_object)
        $relation_conditions
        WITH distinct from, to
        CALL (from, to) {
            MERGE (from) - [r:$type] -> (to)
            RETURN r
        } IN TRANSACTIONS
        RETURN count(r) as count
    '''

    relation_conditions = []
    for _object_type, _object in {"from": from_object, "to": to_object}.items():
        if "relationships" in _object:
            for relationship in _object['relationships']:
                rel_type = relationship["relation_type"]
                related_object = relationship["related_object"]
                related_label = relationship["related_label"]
                relation_conditions.append(
                    f"MATCH ({_object_type}) - [:{rel_type}] - ({related_object}:{related_label})")

    query = Query(
        query_str=query_str,
        template_string_parameters={
            "from_object": from_object["label"],
            "to_object": to_object["label"],
            "type": _type,
            "relation_conditions": "\n".join(relation_conditions)
        }
    )

    res = _db_connection.exec_query(query)
    print(f'→ {res[0]["count"]} (:{from_object["label"]}) - [:{_type}] -> (:{to_object["label"]}) Relationship built')


def extend_relationships(_db_connection, _relationships):
    for _type, _configs in _relationships.items():
        for _config in _configs:
            try:
                extend_relationship(_db_connection, _type, _config)
            except Exception as e:
                print(f"Failed for {_type}: {e}")


#######################################################################
####################### ENRICHMENT METHOD 4 ###########################
######### Infer DF EDGES for objects of specific object type ##########
#######################################################################

def build_df_edges_for_object_type(_db_connection, _object_type):
    """
    Build :DF:* edges for all events related to objects of type :_object_type.
    Creates separate DF edges for each object type and incident event type.
    """

    discover_df_query_str = '''
        :auto
        MATCH (o) - [:IS_OF_TYPE] -> (ot:ObjectType {objectType: $objectType})
        WITH o, ot.objectType as oType
        MATCH (e:Event) -- (o)
        WITH o, oType, e ORDER BY e.timestamp, elementId(e)
        WITH o.sysId as sysId, oType, collect(e) as events
        UNWIND range(0, size(events) - 2) AS index
        WITH events[index] as fromEvent, events[index+1] as toEvent, sysId, oType
        CALL (fromEvent, toEvent, sysId, oType) {
            MERGE (fromEvent) -[rel:DF {objectType:oType, id:sysId}]->(toEvent)
            RETURN count(rel) as count
        } IN TRANSACTIONS
        RETURN sum(count) as count
       '''

    discover_df = Query(query_str=discover_df_query_str,
                        parameters={'objectType': _object_type})

    res = _db_connection.exec_query(discover_df)
    print(f"→ {_object_type} DF creation result: {res[0]['count']}")


def build_df_edges(_db_connection, _object_types):
    create_event_timestamp_index(_db_connection)
    for _object_type in _object_types:
        try:
            build_df_edges_for_object_type(_db_connection, _object_type)
        except Exception as e:
            print(f"Failed to build DFs for {_object_type}: {e}")

#######################################################################
##################### OTHER ENRICHMENT METHODS ########################
#######################################################################
#######################################################################

# Here we will add the other enrichment methods, the ones we used in the analysis are currently in 3_analysis.py