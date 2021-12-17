echo "Start restapi service with env"
echo "PORT:"$PORT
echo "SPORK_JSON_URL:"$SPORK_JSON_URL
echo "ALCHEMY_ENDPOINT:"$ALCHEMY_ENDPOINT
echo "USE_ALCHEMY:"$USE_ALCHEMY
echo "MAX_QUERY_BLOCKS:"$MAX_QUERY_BLOCKS
echo "QUERY_BATCH_SIZE:"$QUERY_BATCH_SIZE

./restapi -port=${PORT} -sporkUrl=${SPORK_JSON_URL} -alchemyEndpoint=${ALCHEMY_ENDPOINT} -alchemyApiKey=${ALCHEMY_API_KEY} -useAlchemy=${USE_ALCHEMY} -maxQueryBlocks=${MAX_QUERY_BLOCKS} -queryBatchSize=${QUERY_BATCH_SIZE}
