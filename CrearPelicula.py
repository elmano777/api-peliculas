import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Iniciando proceso de creación de película",
                "event": event
            }
        }
        print(json.dumps(log_entrada))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Log de éxito
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "tenant_id": tenant_id,
                "uuid": uuidv4,
                "pelicula_datos": pelicula_datos,
                "response_metadata": response.get('ResponseMetadata', {})
            }
        }
        print(json.dumps(log_exito))
        
        # Salida (json)
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
        
    except KeyError as e:
        # Error por clave faltante
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": f"Error: Clave faltante en el evento - {str(e)}",
                "error_type": "KeyError",
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'error': f'Clave faltante: {str(e)}'
        }
        
    except Exception as e:
        # Error general
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": f"Error inesperado: {str(e)}",
                "error_type": type(e).__name__,
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'error': f'Error interno del servidor: {str(e)}'
        }
