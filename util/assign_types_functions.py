# Import logging and surpress warnings
import logging

from util.transformer_functions import create_index

logging.getLogger("neo4j").setLevel(logging.ERROR)
logging.getLogger("pd").setLevel(logging.ERROR)

# Import promg
from promg import Query

first = True


def add_object_type_node(_db_connection, _object_type):
    query_create_ot = '''
        MERGE (ot:ObjectType {objectType: $objectType})
    '''

    _db_connection.exec_query(
        Query(query_str=query_create_ot,
              parameters={'objectType': _object_type}
              )
    )

    query_str = '''
        :auto
        MATCH (ot:ObjectType {objectType: $objectType })
        MATCH (o:$label)
        CALL (o, ot) {
            MERGE (o) - [:IS_OF_TYPE] -> (ot)
            } IN TRANSACTIONS
    '''

    query = Query(
        query_str=query_str,
        parameters={'objectType': _object_type},
        template_string_parameters={"label": _object_type}
    )

    _db_connection.exec_query(query)
    print(f'-> (:ObjectType {{objectType: "{_object_type}"}}) created.')


def add_event_type_node(_db_connection, event_type):
    '''
    This function creates an EventType node (e.g., "IncidentEvent", "InteractionEvent") and then links every node of
    that label in the graph to this type node with an IS_OF_TYPE relationship.
    :param _db_connection:
    :param event_type:
    :return:
    '''
    create_index(_db_connection, 'Event')

    query_create_et = '''
        MERGE (et:EventType {eventType: $eventType})
    '''

    _db_connection.exec_query(
        Query(query_str=query_create_et,
              parameters={'eventType': event_type}
              )
    )

    query_str = '''
        :auto
        MATCH (et:EventType {eventType: $eventType })
        MATCH (e:$label)
        CALL (e, et) {
            MERGE (e) - [:IS_OF_TYPE] -> (et)
            REMOVE e:$label
            SET e:Event
        }
        IN TRANSACTIONS
    '''

    query = Query(
        query_str=query_str,
        parameters={'eventType': event_type},
        template_string_parameters={"label": event_type}
    )

    _db_connection.exec_query(query)
    print(f'-> (:EventType {{eventType: "{event_type}"}}) created.')
