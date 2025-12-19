# Features
- Implemented a multicast mechanism to build a group messenger that can send messages to multiple AVDs (Used fixed set of ports & socket).
- A DB based content provider was used to store the messages.
- Engineered FIFO and total order guarantees. ISIS based algorithm was used for guaranteeing total order.
- Handled failure of an app instance in the middle of execution.
