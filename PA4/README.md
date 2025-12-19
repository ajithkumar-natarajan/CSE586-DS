# Features
- Replicated the functionality of Amazon Dynamo DB by designing a persistent key-value storage.
- Any key-value data inserted will be replicated across multiple databases.
- It supported edge cases such as inserting, querying and failure of an instance in random sequence.
- Once the failed node comes up alive it interacts with other nodes and replicated all the key-value pairs.
