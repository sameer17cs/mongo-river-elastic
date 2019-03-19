'use strict';

class Logger {
    constructor(logLevel) {
        if (logLevel) {
            this.logLevel = logLevel;
        }
        else {
            this.logLevel = 'debug';
        }
    }

    debug(message) {
        if (this.logLevel === 'debug') {
            console.debug(message);
        }
    }

    info(message) {
        if (this.logLevel === 'debug' || this.logLevel === 'info') {
            console.info(message);
        }
    }

    error(message) {
        if (this.logLevel === 'debug' || this.logLevel === 'info' || this.logLevel === 'error') {
            console.error(message);
        }
    }
}

class River {
    constructor(_mongo_db_ref, _es_ref, _collection_index_dict, options) {
        this._mongo_db_ref = _mongo_db_ref;
        this._collection_index_dict = _collection_index_dict;
        this.transporter = new Transporter(_mongo_db_ref, _es_ref, _collection_index_dict, options);
        this._custom_mongo_db_field = 'river';
    }

    transformDocument(operation, doc, timestamp) {
        let _this = this, returnVal;
        switch (operation) {
            case 'insert':
                if (!Array.isArray(doc)) {
                    doc[_this._custom_mongo_db_field] = timestamp;
                    returnVal = doc;
                }
                else {
                    returnVal = doc.map((x) => {
                        x[_this._custom_mongo_db_field] = timestamp;
                        return x;
                    });
                }
                break;

            case 'update':
                doc['$set'][_this._custom_mongo_db_field] = timestamp;
                returnVal = doc;

        }
        return returnVal;
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insert
    insert(collectionName, docs, options, callback) {
        let timestamp = Date.now();
        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        docs = this.transformDocument('insert', docs, timestamp);
        this._mongo_db_ref.collection(collectionName).insert(docs, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insertMany
    insertMany(collectionName, docs, options, callback) {
        let timestamp = Date.now();
        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        docs = this.transformDocument('insert', docs, timestamp);
        this._mongo_db_ref.collection(collectionName).insertMany(docs, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insertOne
    insertOne(collectionName, doc, options, callback) {
        let timestamp = Date.now();
        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        doc = this.transformDocument('insert', doc, timestamp);
        this._mongo_db_ref.collection(collectionName).insertOne(doc, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#update
    update(collectionName, selector, document, options, callback) {
        let timestamp = Date.now();

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        document = this.transformDocument('update', document, timestamp);
        this._mongo_db_ref.collection(collectionName).update(selector, document, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#updateMany
    updateMany(collectionName, filter, update, options, callback) {
        let timestamp = Date.now();

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        update = this.transformDocument('update', update, timestamp);
        this._mongo_db_ref.collection(collectionName).updateMany(filter, update, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#updateOne
    updateOne(collectionName, filter, update, options, callback) {

        let timestamp = Date.now();

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        update = this.transformDocument('update', update, timestamp);
        this._mongo_db_ref.collection(collectionName).updateOne(filter, update, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#deleteOne
    deleteOne(collectionName, filter, options, callback) {

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }

        let primaryKeyField = this._collection_index_dict[collectionName].primaryKeyField;

        let projection = {projection: {_id: 0}};
        projection['projection'][primaryKeyField] = 1;

        //find primaryKey of all docs to be deleted
        this._mongo_db_ref.collection(collectionName).findOne(filter, projection, (err, findResponse) => {
            if (err) {
                return callback(err, null);
            }
            else {
                this._mongo_db_ref.collection(collectionName).deleteOne(filter, options, (err, response) => {
                    if (!err) {
                        this.transporter.delete(collectionName, [findResponse[primaryKeyField]]);
                    }
                    return callback(err, response);
                })
            }
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#deleteMany
    deleteMany(collectionName, filter, options, callback) {

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }

        let primaryKeyField = this._collection_index_dict[collectionName].primaryKeyField;

        let projection = {projection: {_id: 0}};
        projection['projection'][primaryKeyField] = 1;

        //find primaryKey of all docs to be deleted
        this._mongo_db_ref.collection(collectionName).find(filter, projection).toArray((err, findResponse) => {
            if (err) {
                return callback(err, null);
            }
            else {
                this._mongo_db_ref.collection(collectionName).deleteMany(filter, options, (err, response) => {
                    if (!err) {
                        this.transporter.delete(collectionName, findResponse.map(x => x[primaryKeyField]));
                    }
                    return callback(err, response);
                })
            }
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#replaceOne
    replaceOne(collectionName, filter, doc, options, callback) {
        let timestamp = Date.now();

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }
        doc = this.transformDocument('insert', doc, timestamp);
        this._mongo_db_ref.collection(collectionName).replaceOne(filter, doc, options, (err, response) => {
            if (!err) {
                this.transporter.sync(collectionName, timestamp);
            }
            return callback(err, response);
        })
    }

    //http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#bulkWrite
    bulkWrite(collectionName, operations, options, callback) {
        let timestamp = Date.now();
        let _this = this;

        if (typeof callback !== "function") {
            callback = options;
            options = null;
        }

        if (!Array.isArray(operations)) {
            operations = [operations];
        }

        //local object
        let transporterBulkOperations = {sync: timestamp, delete: []};

        Promise.all(operations.map((x, index) => {
            return new Promise((resolve,reject) => {
                let op = Object.keys(x)[0];
                switch (op) {
                    case 'insertOne':
                        operations[index][op]['document'] = _this.transformDocument('insert', operations[index][op]['document'], timestamp);
                        resolve();
                        break;

                    case 'updateOne':
                        operations[index][op]['update'] = _this.transformDocument('update', operations[index][op]['update'], timestamp);
                        resolve();
                        break;

                    case 'updateMany':
                        operations[index][op]['update'] = _this.transformDocument('update', operations[index][op]['update'], timestamp);
                        resolve();
                        break;

                    case 'deleteOne':
                        var primaryKeyField = _this._collection_index_dict[collectionName].primaryKeyField;
                        var projection = {_id: 0};
                        projection[primaryKeyField] = 1;
                        _this._mongo_db_ref.collection(collectionName).findOne(operations[index][op]['filter'], {$projection: projection}, (err, doc) => {
                            if (err) {
                                reject(err);
                            }
                            else {
                                if (doc) {
                                    transporterBulkOperations.delete = transporterBulkOperations.delete.concat([doc[primaryKeyField]]);
                                }
                                resolve();
                            }
                        });
                        break;

                    case 'deleteMany':
                        primaryKeyField = _this._collection_index_dict[collectionName].primaryKeyField;
                        projection = {_id: 0};
                        projection[primaryKeyField] = 1;
                        _this._mongo_db_ref.collection(collectionName).find(operations[index][op]['filter'], {$projection: projection}).toArray((err, docs) => {
                            if (err) {
                                reject(err);
                            }
                            else {
                                if (docs.length > 0){
                                    transporterBulkOperations.delete = transporterBulkOperations.delete.concat(docs.map(x => x[primaryKeyField]));
                                }
                                resolve()
                            }
                        });
                        break;

                    case 'replaceOne':
                        operations[index][op]['replacement'] = _this.transformDocument('insert', operations[index][op]['replacement'], timestamp);
                        resolve();
                        break;
                }
            })
        }))
            .then(() => {
                this._mongo_db_ref.collection(collectionName).bulkWrite(operations, options, (err, response) => {
                    if (!err) {
                        this.transporter.bulkWrite(collectionName, transporterBulkOperations);
                    }
                    return callback(err, response);
                })
            })
    }
}

class Transporter {
    constructor(_mongo_db_ref, _es_ref, _collection_index_dict, options) {
        this._es_ref = _es_ref;
        this._mongo_db_ref = _mongo_db_ref;
        this._collection_index_dict = _collection_index_dict;
        this.logger = new Logger(options.logLevel);

        let customField = 'river';
        this._custom_mongo_db_field = customField;

        //ensure index on transport field
        for (let eachCollection in _collection_index_dict) {
            _mongo_db_ref.collection(eachCollection).createIndex(customField);
        }
    }


    //docs can be object or array of object
    sync(collectionName, timestamp) {
        let _this = this;
        let query = {};
        query[this._custom_mongo_db_field] = {$gte: timestamp};
        this._mongo_db_ref.collection(collectionName).find(query, {
            projection: {
                river: 0,
                _id: 0
            }
        }).toArray()
            .then((docs) => {
                let index = this._collection_index_dict[collectionName].index;
                let type = this._collection_index_dict[collectionName].type;
                let primaryKeyField = this._collection_index_dict[collectionName].primaryKeyField;
                let insertBody = [];
                docs.map((x) => {
                    // action description
                    insertBody.push({index: {_index: index, _type: type, _id: x[primaryKeyField]}});
                    // the document to index
                    insertBody.push(x)

                });
                this._es_ref.bulk({
                    body: insertBody
                }, function (err, resp) {
                    if (err) {
                        _this.logger.error(err);
                        return _this.sync(collectionName, timestamp)
                    }
                    else if (resp.errors) {
                        _this.logger.error(JSON.stringify(resp.items));
                    }
                    else {
                        _this.logger.debug(`[Transporter] Synced document count: ${docs.length}`);
                    }
                });
            })
            .catch((err) => {
                _this.logger.error(err);
                return _this.sync(collectionName, timestamp)
            });
    }

    delete(collectionName, ids) {
        let _this = this;
        let index = this._collection_index_dict[collectionName].index;
        let type = this._collection_index_dict[collectionName].type;

        if (ids.length) {
            Promise.all(ids.map((x) => {
                return new Promise((resolve, reject) => {
                    this._es_ref.delete({index: index, type: type, id: x}, function (err, resp) {
                        if (err) {
                            return reject(err);
                        }
                        else if (resp.errors) {
                            _this.logger.error(JSON.stringify(resp.items));
                            return resolve();
                        }
                        else {
                            return resolve();
                        }
                    })
                })
            }))
                .then(() => {
                    _this.logger.debug(`[Transporter] Removed document count: ${ids.length}`);
                })
                .catch((err) => {
                    _this.logger.error(err);
                    _this.delete(collectionName, ids);
                })
        }
    }

    bulkWrite(collectionName, transporterBulkOperations) {
        this.sync(collectionName, transporterBulkOperations.sync);
        this.delete(collectionName, transporterBulkOperations.delete);

    }
}

module.exports = River;