YUI.add("yuidoc-meta", function(Y) {
   Y.YUIDoc = { meta: {
    "classes": [
        "AuthReq",
        "Client",
        "CommandBase",
        "DefaultNodeManager",
        "DeleteIndex",
        "DeleteIndex.Builder",
        "DeleteValue",
        "DeleteValue.Builder",
        "FetchBucketProps",
        "FetchBucketProps.Builder",
        "FetchCounter",
        "FetchCounter.Builder",
        "FetchIndex",
        "FetchIndex.Builder",
        "FetchMap",
        "FetchMap.Builder",
        "FetchSchema",
        "FetchSchema.Builder",
        "FetchSet",
        "FetchSet.Builder",
        "FetchValue",
        "FetchValue.Builder",
        "ListBuckets",
        "ListBuckets.Builder",
        "ListKeys",
        "ListKeys.Builder",
        "MapReduce",
        "NodeManager",
        "Ping",
        "RiakCluster",
        "RiakCluster.Builder",
        "RiakConnection",
        "RiakNode",
        "RiakNode.Builder",
        "RiakObject",
        "Search",
        "Search.Builder",
        "SecondaryIndexQuery",
        "SecondaryIndexQuery.Builder",
        "StartTls",
        "StoreBucketProps",
        "StoreBucketProps.Builder",
        "StoreIndex",
        "StoreIndex.Builder",
        "StoreSchema",
        "StoreSchema.Builder",
        "StoreValue",
        "StoreValue.Builder",
        "UpdateCounter",
        "UpdateCounter.Builder",
        "UpdateMap",
        "UpdateMap.Builder",
        "UpdateMap.MapOperation",
        "UpdateSet",
        "UpdateSet.Builder"
    ],
    "modules": [
        "CRDT",
        "Client",
        "Core",
        "KV",
        "MR",
        "YZ"
    ],
    "allModules": [
        {
            "displayName": "Client",
            "name": "Client",
            "description": "Provides the Client class"
        },
        {
            "displayName": "Core",
            "name": "Core",
            "description": "Provides the classes that make up the core of the client."
        },
        {
            "displayName": "CRDT",
            "name": "CRDT",
            "description": "Provides all the commands for Riak CRDTs (Conflict-Free Replicated Data Type)"
        },
        {
            "displayName": "KV",
            "name": "KV",
            "description": "Provides all the commands for Riak Key-Value operations."
        },
        {
            "displayName": "MR",
            "name": "MR",
            "description": "Provides the commands for Riak Map-Reduce"
        },
        {
            "displayName": "YZ",
            "name": "YZ",
            "description": "Provides all the commands for Riak Search 2.0 (Yokozuna/Solr)"
        }
    ]
} };
});