# Import logging and surpress warnings
import logging
from typing import List

from util.assign_types_functions import add_object_type_node
from util.transformer_functions import create_index, create_event_timestamp_index

logging.getLogger("neo4j").setLevel(logging.ERROR)
logging.getLogger("pd").setLevel(logging.ERROR)

import pandas as pd

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

def build_df_edges(_db_connection, _object_type: str, _event_types: List[str], _timestamp_fields: List[str] = None):
    """
    Build :DF:* edges for all events related to objects of type :_object_type.
    Creates separate DF edges for each object type and incident event type.
    """

    if _timestamp_fields is None:
        _timestamp_fields = ['timestamp']

    for timestamp_field in _timestamp_fields:
        create_event_timestamp_index(_db_connection,
                                     _label='Event',
                                     _timestamp_field=timestamp_field)


        create_event_timestamp_index(_db_connection,
                                     _label='HighLevelEvent',
                                     _timestamp_field=timestamp_field)


    get_all_events_per_timestamp_field_attribute = "\n UNION ALL \n".join([
        f'''
                MATCH (e:Event|HighLevelEvent) -- (o)
                MATCH (e) - [:IS_OF_TYPE] -> (et:EventType)
                WHERE et.eventType in $eventTypes AND e.{timestamp_field} IS NOT NULL
                RETURN e, e.{timestamp_field} as timestamp
            ''' for timestamp_field in _timestamp_fields
    ])

    discover_df_query_str = '''
        :auto
        MATCH (o) - [:IS_OF_TYPE] -> (ot:ObjectType {objectType: $objectType})
        WITH o, ot.objectType as oType
        CALL (o) {
            $get_all_events_per_timestamp_field_attribute
        }
        WITH o, oType, e ORDER BY timestamp, elementId(e)
        WITH o.sysId as sysId, oType, collect(e) as events
        UNWIND range(0, size(events) - 2) AS index
        WITH events[index] as fromEvent, events[index+1] as toEvent, sysId, oType
        WHERE fromEvent <> toEvent
        WITH fromEvent, toEvent, sysId, oType
        CALL (fromEvent, toEvent, sysId, oType) {
            MERGE (fromEvent) -[rel:DF {objectType:oType, id:sysId}]->(toEvent)
            RETURN count(rel) as count
        } IN TRANSACTIONS
        RETURN sum(count) as count
       '''

    discover_df = Query(query_str=discover_df_query_str,
                        parameters={
                            'objectType': _object_type,
                            'eventTypes': _event_types
                        },
                        template_string_parameters={
                            'get_all_events_per_timestamp_field_attribute': get_all_events_per_timestamp_field_attribute
                        })

    res = _db_connection.exec_query(discover_df)
    print(f"→ {_object_type} DF creation result: {res[0]['count']}")


#######################################################################
##################### STANDARD PM TECHNIQUES ##########################
#######################################################################
#######################################################################

def get_variant_length_statistics(_db_connection, _object_type: str, _event_types: List[str]):
    q_variants_str = '''
        MATCH (:ObjectType {objectType: $objectType}) <- [:IS_OF_TYPE] - (o)
        MATCH (o) -- (e:Event|HighLevelEvent) - [:IS_OF_TYPE] -> (et:EventType)
        WHERE et.eventType IN $eventTypes
        WITH o.sysId as objectId, count(e) as number_of_events
        RETURN min(number_of_events) AS min_length, max(number_of_events) AS max_length, 
        avg(number_of_events) AS avg_length, stDev(number_of_events) AS stDev_length
    '''

    q_variants = Query(query_str=q_variants_str,
                       parameters={
                           'objectType': _object_type,
                           'eventTypes': _event_types
                       })

    _result = pd.DataFrame(_db_connection.exec_query(q_variants))
    return _result


def get_activity_set_variants(_db_connection, _object_type, _event_types):
    # get the bag variants on the high_level
    q_set_activity_variants_str = '''
        MATCH (:ObjectType {objectType: $objectType}) <- [:IS_OF_TYPE] - (o) -- (e:Event|HighLevelEvent) - [
        :IS_OF_TYPE] -> (et:EventType)
        WHERE et.eventType IN $eventTypes
        WITH o, e.activity AS activity ORDER BY activity
        WITH o, collect(distinct activity) as set_variant
        RETURN ltrim(reduce(initial = "", activity in set_variant | initial + " - (" + activity + ")" ), " - " ) as 
        set_variant, count(o) as count_objects order by count_objects DESC
    '''

    q_set_activity_variants = Query(query_str=q_set_activity_variants_str,
                                    parameters={
                                        'objectType': _object_type,
                                        'eventTypes': _event_types
                                    })

    _result = pd.DataFrame(_db_connection.exec_query(q_set_activity_variants))
    _result['%_set_variant'] = round(
        _result.groupby(['set_variant']).count_objects.transform("sum") / sum(_result['count_objects']) * 100, 2)
    return _result


#######################################################################
##################### OTHER ENRICHMENT METHODS ########################
#######################################################################
#######################################################################

#######################################################################
##################### ENRICH WITH HIGH LEVEL EVENTS ###################
#######################################################################
#######################################################################

def infer_start_event(_db_connection, _object_type: str, _event_types: List[str]):
    # infer start and end events for each object type
    q_start_event = '''
        :auto
        // Infer start event of an object
        MATCH (o) - [:IS_OF_TYPE] -> (ot:ObjectType {objectType: $objectType})
        MATCH (o)<-[]-(e:Event|HighLevelEvent) - [:IS_OF_TYPE] -> (et:EventType)
        WHERE NOT ()-[:DF {id:o.sysId}]->(e) AND et.eventType IN $eventTypes
        CALL (o, e){
            MERGE (o)<-[rel:START]-(e)
            RETURN rel
        } IN TRANSACTIONS
        RETURN count(rel) as count
    '''

    q_start_event_result = Query(
        query_str=q_start_event,
        parameters={
            "objectType": _object_type,
            "eventTypes": _event_types
        }
    )

    res = _db_connection.exec_query(q_start_event_result)

    print(f'→ Inferred Start Events for {res[0]["count"]} objects ({_object_type})')


def infer_end_event(_db_connection, _object_type: str, _event_types: List[str]):
    # infer start and end events for each object type
    q_end_event = '''
        :auto
        // Infer start event of an object
        MATCH (o) - [:IS_OF_TYPE] -> (ot:ObjectType {objectType: $objectType})
        MATCH (o)<-[]-(e:Event|HighLevelEvent) - [:IS_OF_TYPE] -> (et:EventType)
        WHERE NOT (e)-[:DF {id:o.sysId}]->() AND et.eventType IN $eventTypes
        CALL (o, e){
            MERGE (o)<-[rel:END]-(e)
            RETURN rel
        } IN TRANSACTIONS
        RETURN count(rel) as count
    '''

    q_end_event_result = Query(
        query_str=q_end_event,
        parameters={
            "objectType": _object_type,
            "eventTypes": _event_types
        }
    )

    res = _db_connection.exec_query(q_end_event_result)

    print(f'→ Inferred End Events for {res[0]["count"]} objects ({_object_type})')


def infer_high_level_events_based_on_start_and_end_events(_db_connection, _object_type: str, _hle_event_type: str):
    create_index(_db_connection, 'HighLevelEvent')
    create_event_timestamp_index(_db_connection, 'HighLevelEvent', 'startTime')
    create_event_timestamp_index(_db_connection, 'HighLevelEvent', 'endTime')

    # build high-level events
    q_build_high_level_event_str = '''
        :auto
        MATCH (n) - [:IS_OF_TYPE] -> (ot:ObjectType {objectType: $objectType})
        MATCH (eStart:Event)-[st:START]->(n)<-[en:END]-(eEnd:Event)
        WITH DISTINCT eStart, eEnd, n
        CALL (eStart, eEnd, n) {
            MERGE (h:HighLevelEvent {sysId: "HLE_" + eStart.sysId + "_" + eEnd.sysId})
            MERGE (h_et:EventType {eventType: $hleEventType})
            MERGE (h) - [:IS_OF_TYPE] -> (h_et)
            ON CREATE SET h.startTime=eStart.timestamp, h.endTime=eEnd.timestamp, h.activity=$objectType
            MERGE (h)-[:START]->(eStart)
            MERGE (h)-[:END]->(eEnd)
            MERGE (h) - [c:CORR] -> (n)
            RETURN h
        } IN TRANSACTIONS
        RETURN count(h) as count
    '''

    q_build_high_level_event_result = Query(
        query_str=q_build_high_level_event_str,
        parameters={
            "objectType": _object_type,
            "hleEventType": _hle_event_type
        }
    )

    res = _db_connection.exec_query(q_build_high_level_event_result)
    print(f'→ Inferred {res[0]["count"]} (:HighLevelEvent) of type {_hle_event_type} for ObjectType ({_object_type})')

