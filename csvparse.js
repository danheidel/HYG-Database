var fs = require('fs');
var extend = require('util')._extend;
var csv = require('csv');
var mariadb = require('mysql');
var fileName = '/hygfull.csv';

function mariaConHandler() {
        mariaConnection = mariadb.createConnection({
                host:   "localhost",
                user:   "root",
                password:"Ih35MV9XqLcS",
                database:"HYG",
        });

        mariaConnection.connect(function(err){
                if(err){
                        console.log('error while connecting to db: ', err);
                        setTimeout(mariaConHandler, 2500);
                }else{
			console.log('database connection opened');
		}
        });

        mariaConnection.on('error', function(err){
                console.log('db error: ', err);
                if(err.code ==='PROTOCOL_CONNECTION_LOST'){
                        mariaConHandler();
                } else {
                        throw err;
                }
        });
}

mariaConHandler();

var csvArray = [];
var csvEditArray = [];

csv().from.path(__dirname + fileName, {delimiter:','})
.on('record', function(row, index){
  var tempData = {
    StarID: row[0],
    Hip: row[1]==''?null:row[1],
    HD: row[2]==''?null:row[2],
    HR: row[3]==''?null:row[3],
    Gliese: row[4]==''?null:row[4],
    BayerFlamstead: row[5]==''?null:row[5],
    ProperName: row[6]==''?null:row[6],
    RA: row[7]==''?null:row[7],
    Dec_: row[8]==''?null:row[8],
    Distance: row[9]==''?null:row[9],
    Mag: row[10]==''?null:row[10],
    AbsMag: row[11]==''?null:row[11],
    Spectrum: row[12]==''?null:row[12],
    ColorIndex: row[13]==''?null:row[13],
  };
  
  //offset by one to eliminate column headers
  if(index > 0) csvArray.push(tempData);
  
  //if distance data is valid, add to edited list and calulate Spectral type from ColorIndex
  if(tempData.Distance != 10000000 && index > 0){
    var tempEditData = extend({}, tempData);
    tempEditData.CalcSpectrum = convertSpecData(tempEditData.ColorIndex);
    csvEditArray.push(tempEditData);
  }
  
  if(index % 1000 == 0){console.log(index);}
})
.on('end', function(count){
  console.log(count + ' rows added');
  console.log('array has: ' + csvArray.length + ' entries');
  writeToDb(0);
})
.on('error', function(error){
  console.log(error.message);
});

function writeToDb(rep){
  if(rep < csvArray.length){
    dbInsert(rep, function(){ writeToDb(rep + 1);});
  }else{
    console.log('wrote ' + rep + ' rows to db');
    //chain to 2nd set of db writes
    WriteEditToDb(0);
  }
}

function dbInsert(rep, callback){
  mariaConnection.query('INSERT INTO full SET ?', csvArray[rep], function(err, result){
    if(err){console.log(err);}
    if(rep%100 == 0){console.log(rep);}
    callback();
  });  
}

function writeEditToDb(rep){
  if(rep < csvEditArray.length){
    dbEditInsert(rep, function(){ writeEditToDb(rep + 1);});
  }else{
    console.log('wrote ' + rep + ' edited rows to db');
    closeDbConnection();
  }
}

function dbEditInsert(rep, callback){
  mariaConnection.query('INSERT INTO good_dist SET ?', csvEditArray[rep], function(err, result){
    if(err){console.log(err);}
    if(rep%100 == 0){console.log(rep);}
    callback();
  });
}

function closeDbConnection(){
  mariaConnection.end(function(err){
    if(err){
      console.log(err);
    }
    console.log('database connection closed');
  });
}

//data from: http://www-int.stsci.edu/~inr/intrins.html
var spectrumConv = [
  {'bv':-0.30,sp:'B0.0'},
  {'bv':-0.28,sp:'B0.5'},
  {'bv':-0.26,sp:'B1.0'},
  {'bv':-0.25,sp:'B1.5'},
  {'bv':-0.24,sp:'B2.0'},
  {'bv':-0.22,sp:'B2.5'},
  {'bv':-0.20,sp:'B3.0'},
  {'bv':-0.19,sp:'B3.5'},
  {'bv':-0.18,sp:'B4.0'},
  {'bv':-0.17,sp:'B4.5'},
  {'bv':-0.16,sp:'B5.0'},
  {'bv':-0.14,sp:'B6.0'},
  {'bv':-0.13,sp:'B7.0'},
  {'bv':-0.12,sp:'B7.5'},
  {'bv':-0.11,sp:'B8.0'},
  {'bv':-0.09,sp:'B8.5'},
  {'bv':-0.07,sp:'B9.0'},
  {'bv':-0.04,sp:'B9.5'},
  {'bv':-0.01,sp:'A0.0'},
  {'bv':0.02,sp:'A1.0'},
  {'bv':0.05,sp:'A2.0'},
  {'bv':0.08,sp:'A3.0'},
  {'bv':0.12,sp:'A4.0'},
  {'bv':0.15,sp:'A5.0'},
  {'bv':0.17,sp:'A6.0'},
  {'bv':0.20,sp:'A7.0'},
  {'bv':0.27,sp:'A8.0'},
  {'bv':0.30,sp:'A9.0'},
  {'bv':0.32,sp:'F0.0'},
  {'bv':0.34,sp:'F1.0'},
  {'bv':0.35,sp:'F2.0'},
  {'bv':0.45,sp:'F5.0'},
  {'bv':0.53,sp:'F8.0'},
  {'bv':0.60,sp:'G0.0'},
  {'bv':0.63,sp:'G2.0'},
  {'bv':0.65,sp:'G3.0'},
  {'bv':0.68,sp:'G5.0'},
  {'bv':0.74,sp:'G8.0'},
  {'bv':0.81,sp:'K0.0'},
  {'bv':0.86,sp:'K1.0'},
  {'bv':0.92,sp:'K2.0'},
  {'bv':0.95,sp:'K3.0'},
  {'bv':1.00,sp:'K4.0'},
  {'bv':1.15,sp:'K5.0'},
  {'bv':1.33,sp:'K7.0'},
  {'bv':1.37,sp:'M0.0'},
  {'bv':1.47,sp:'M1.0'},
  {'bv':1.47,sp:'M2.0'},
  {'bv':1.50,sp:'M3.0'},
  {'bv':1.52,sp:'M4.0'},
];

function convertSpecData(iBV){
  for(var rep=0;rep<spectrumConv.length;rep++){
    if(iBV <= spectrumConv[rep].bv){
      return spectrumConv[rep].sp;
    }
  }
  return spectrumConv[spectrumConv.length - 1].bv;
}
