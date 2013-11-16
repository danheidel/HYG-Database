var db = require('mysql');

exports.dbConHandler = function(dbConfig){
  dbConnection = db.createConnection(dbConfig);
  
  dbConnection.connect(function(err){
    if(err){
      console.log('error while connecting to db: ', err);
      setTimeout(dbConHandler, 2500);
    }else{
      console.log('database connection opened to: ' + dbConnection._protocol._config.database);
    }
  });

  dbConnection.on('error', function(err){
    console.log('db error: ', err);
    if(err.code === 'PROTOCOL_CONNECTION_LOST'){
      dbConHandler(); //if connection closes, reopen
    }else{
      throw err;
    }
  });
  return dbConnection;
}

exports.closeDbConnection = function(dbConnection){
  dbConnection.end(function(err){
    if(err){
      console.log(err);
    }
    console.log('database connection to ' + dbConnection._protocol._config.database  + ' closed');
  });
}
