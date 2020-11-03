import logging
from os import path

from fdk import response

from borneo import (Regions, NoSQLHandle, NoSQLHandleConfig,
                    PrepareRequest, QueryRequest)
from borneo.iam import SignatureProvider

def handler(ctx, data: io.BytesIO = None):
    
    return_citizens = []
    
    try:
        provider = SignatureProvider(
        tenant_id='Your Tenant OCID',
        user_id='Your User OCID',
        private_key='location of Pem file',
        fingerprint='The fingerprint for your key pair goes here',
        pass_phrase='The pass phrase for your key goes here')
        compartment = 'Your Compartment Name Goes Here'
        config = NoSQLHandleConfig(Regions.US_ASHBURN_1, provider)
        config.set_default_compartment(compartment)
        logger = logging.getLogger('Citizens')
        logger.setLevel(logging.WARNING)
        config.set_logger(logger)
        handle = NoSQLHandle(config)
        table_name = 'Citizens'

        ## Prepare select statement#
        statement = 'select * from ' + table_name
        request = PrepareRequest().set_statement(statement)
        prepared_result = handle.prepare(request)
        
        ## Query, using the prepared statement#
        request = QueryRequest().set_prepared_statement(prepared_result)
        
        while True:
            result = handle.query(request)
            for r in result.get_results():
                return_citizens.append(dict(r))
            if request.is_done():
                break
        
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    logging.getLogger().info("Inside OCI function")
    return response.Response(
        ctx, response_data=json.dumps(return_citizens),
        headers={"Content-Type": "application/json"}
    )
