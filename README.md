# Mango Chainsaw

Mango Chainsaw is a "database"?

It is designed to store loosely related objects in collections (called Namespaces) using Labels to index them. Powered by [sled](https://sled.rs/), it is transactional. It supports storing almost anything. 

Features:
* Fully transactional insert and delete operations
* Query using Labels
* Get stuff out of it again!
* Unreliable and mostly untested