const MongoClient = require('mongodb').MongoClient;


// Connection URL
const url = 'XXX';

// Database Name
const dbName = 'vmarket';
const db_transaction = 'transactions';

let beforeTest = (db) => {
  return db.collection('accounts').deleteMany({}).then(() => {
    return db.collection(db_transaction).deleteMany({}).then(() => {
      return db.collection('accounts').insertMany(
       [
         { _id: "A", balance: 1000, pendingTransactions: [] },
         { _id: "B", balance: 1000, pendingTransactions: [] }
       ]
     );      
    });     
  });
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

let retriveTransactionToStart = (db) => {
  return db.collection(db_transaction).findOne({
    state: "initial"
  }).then((docs) => {
    if (!docs) return Promise.reject('docs is empty');
    return docs;
  });    
};

let updateStateToPending = (db, t) => {
  // Update transaction state to pending
  return db.collection(db_transaction).updateOne(
    { _id: t._id, state: "initial" },
    {
      $set: { state: "pending" },
      $currentDate: { lastModified: true }
    }
  ).then(() => t);   
};

let updateStateToApplied = (db, t) => {
  // Update transaction state to applied
  return db.collection(db_transaction).updateOne(
    { _id: t._id, state: "pending" },
    {
      $set: { state: "applied" },
      $currentDate: { lastModified: true }
    }
  ).then(() => t);    
};

let updateStateToDone = (db, t) => {
  // Update transaction state to done
  return db.collection(db_transaction).updateOne(
    { _id: t._id, state: "applied" },
    {
      $set: { state: "done" },
      $currentDate: { lastModified: true }
    }
  ).then(() => t);   
};

let applyTransaction = (db, t) => {
  return Promise.resolve().then(() => {
    return db.collection('accounts').updateOne(
     { _id: t.source, pendingTransactions: { $ne: t._id } },
     { $inc: { balance: -t.value }, $push: { pendingTransactions: t._id } }
    ).then(() => t);
  }).then((t) => {
    return db.collection('accounts').updateOne(
     { _id: t.destination, pendingTransactions: { $ne: t._id } },
     { $inc: { balance: t.value }, $push: { pendingTransactions: t._id } }
    ).then(() => t);         
  }); 
};


let commitTransaction = (db, t) => {
  return Promise.resolve().then(() => {
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
  });
};

let doTransactions = (db) => {
  return Promise.resolve().then(() => {
    // Retrieve the transaction to start
    return retriveTransactionToStart(db);
  }).then((t) => {
    return updateStateToPending(db, t);
  }).then((t) => {
    return applyTransaction(db, t);
  }).then((t)=>{
    throw new Error('Exception One'); // throw exception
  }).then((t) => {
    return updateStateToApplied(db, t);
  }).then((t) => {
   return commitTransaction(db, t);
  }).then((t) => {
    return updateStateToDone(db, t);
  });
};

let afterTest = (db) => {
  db.collection('accounts').deleteMany({});   
  // db.collection(db_transaction).deleteMany({});   
  return Promise.resolve();
};

let printAccounts = (db) => {
  return db.collection('accounts').find({}).toArray((err, docs)=>{
    console.log(docs); 
  });
};

let startCancelTransaction = (db, t) => {
  return db.collection(db_transaction).updateOne(
   { _id: t._id, state: "pending" },
   {
     $set: { state: "canceling" },
     $currentDate: { lastModified: true }
   }
  );    
};

let revertTransaction = (db, t) => {
  return Promise.resolve().then(() => {
    return db.collection('accounts').updateOne(
      { _id: t.destination, pendingTransactions: t._id },
      {
        $inc: { balance: -t.value },
        $pull: { pendingTransactions: t._id }
      }).then(() => t);
  }).then((t) => {
    return db.collection('accounts').updateOne(
      { _id: t.source, pendingTransactions: t._id },
      {
        $inc: { balance: t.value},
        $pull: { pendingTransactions: t._id }
      }).then(() => t);      
  }); 
};

let finishCancelTransaction = (db, t) => {
  return db.collection(db_transaction).updateOne(
   { _id: t._id, state: "canceling" },
   {
     $set: { state: "cancelled" },
     $currentDate: { lastModified: true }
   });    
};

let rollbackTransaction = (db, t) => {
  console.log('rollback transaction:' + t._id);
  return Promise.resolve().then(()=>{
    return startCancelTransaction(db, t);
  }).then(() => {
    return revertTransaction(db, t);  
  }).then(()=>{
    return finishCancelTransaction(db, t);
  })
};

let loopCheckPendingTransaction = (db) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      var dateThreshold = new Date();
      dateThreshold.setSeconds(dateThreshold.getSeconds() - 10);
      db.collection(db_transaction).findOne({
        state: "pending",
        lastModified: { $lt: dateThreshold } 
      }).then((doc) => {
        return resolve(doc);
      }); 
    }, 15000);
  });
};

let main = () => {
  // Use connect method to connect to the server
  MongoClient.connect(url)
  .then((client) => {
    console.log("Connected successfully to server");
    return client.db(dbName);
  })
  .then((db) => {
    loopCheckPendingTransaction(db).then((t)=>{
      console.log('before rollback==>');
      printAccounts(db);
      return t;
    }).then((t) => {   
      return rollbackTransaction(db, t);
    }).then(() => {
      console.log('after rollback==>')
      return printAccounts(db);   
    });
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
      //afterTest(db);      
      throw err;
    });
  });  
};
main();