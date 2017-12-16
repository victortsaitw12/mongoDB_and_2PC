const MongoClient = require('mongodb').MongoClient;


// Connection URL
const url = 'xxxx';

// Database Name
const dbName = 'vmarket';
const db_transaction = 'transactions';

let beforeTest = (db) => {
  return db.collection('accounts').insertMany(
     [
       { _id: "A", balance: 1000, pendingTransactions: [] },
       { _id: "B", balance: 1000, pendingTransactions: [] }
     ]
  );
};

let newTransaction = (db) => {
  return db.collection(db_transaction).insertOne({
      // _id: 1, 
      source: "A", 
      destination: "B", 
      value: 100, 
      state: "initial", 
      lastModified: new Date()
  });
};

let doTransactions = (db) => {
  return Promise.resolve().then(() => {
    // Retrieve the transaction to start
    return db.collection(db_transaction).findOne({
      state: "initial"
    }).then((docs) => {
      if (!docs) return Promise.reject('docs is empty');
      return docs;
    });
  }).then((t) => {
    // Update transaction state to pending
    return db.collection(db_transaction).updateOne(
      { _id: t._id, state: "initial" },
      {
        $set: { state: "pending" },
        $currentDate: { lastModified: true }
      }
    ).then(() => t);
  }).then((t) => {
    // Apply the the transaction to both accounts.
    return db.collection('accounts').updateOne(
     { _id: t.source, pendingTransactions: { $ne: t._id } },
     { $inc: { balance: -t.value }, $push: { pendingTransactions: t._id } }
    ).then(() => t);
  }).then((t) => {
    return db.collection('accounts').updateOne(
     { _id: t.destination, pendingTransactions: { $ne: t._id } },
     { $inc: { balance: t.value }, $push: { pendingTransactions: t._id } }
    ).then(() => t);
  }).then((t) => {
    // Update transaction state to applied
    return db.collection(db_transaction).updateOne(
     { _id: t._id, state: "pending" },
     {
       $set: { state: "applied" },
       $currentDate: { lastModified: true }
     }
    ).then(() => t);
  }).then((t) => {
    // Update both accounts' list of pending transactions
    return db.collection('accounts').updateOne(
     { _id: t.source, pendingTransactions: t._id },
     { $pull: { pendingTransactions: t._id } }
    ).then(() => t);
  }).then((t) => {
    return db.collection('accounts').updateOne(
     { _id: t.destination, pendingTransactions: t._id },
     { $pull: { pendingTransactions: t._id } }
    ).then(() => t);
  }).then((t) => {
    // Update transaction state to done
    return db.collection(db_transaction).updateOne(
     { _id: t._id, state: "applied" },
     {
       $set: { state: "done" },
       $currentDate: { lastModified: true }
     }
    ).then(() => t);
  });
};

let afterTest = (db) => {
  db.collection('accounts').deleteMany({});   
  db.collection(db_transaction).deleteMany({});   
  return Promise.resolve();
};

let printAccounts = (db) => {
  return db.collection('accounts').find({}).toArray((err, docs)=>{
    console.log(docs); 
  });
};

let main = () => {
  // Use connect method to connect to the server
  MongoClient.connect(url)
  .then((client) => {
    console.log("Connected successfully to server");
    return client.db(dbName);
  }).then((db) => {
    beforeTest(db).then(() => {
      return newTransaction(db);
    }).then(() => {
      return printAccounts(db);
    }).then(() => {
      return doTransactions(db);
    }).then(() => {
      return printAccounts(db);
    }).then(() => {
      afterTest(db);
    }).catch((err) => {
      afterTest(db);      
      throw err;
    });
  });    
};
main();