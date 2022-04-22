const { default: mongoose } = require("mongoose");
const Transaction = require('./models/transaction')
const Miners = require('./models/miners');
const UpdateStatus = require("./models/updateStatus");

const axios = require('axios');
// const { add } = require('cheerio/lib/api/traversing');
const decimalBase = 1000000000000000000;
var mongo = require('mongodb');
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

mongoose.connect("mongodb://localhost/filfox")
//1646110800  三月1日  1650550500


const createUpdateSta = async(address,maxStamp,minStamp) =>{
    var lastUpdate = new Date().getTime()
     await  UpdateStatus.create({address:address,maxTimestamp: maxStamp,minTimestamp:minStamp,lastUpdate:lastUpdate})
}
const updateUpdateSta = async(address,maxStamp,minStamp) =>{
    console.log(maxStamp,minStamp)
    var lastUpdate = new Date().getTime()
    var str1 = new Date(lastUpdate)
    console.log(lastUpdate)
    await  UpdateStatus.findOneAndUpdate({address:address},{maxTimestamp: maxStamp,minTimestamp:minStamp,lastUpdate:lastUpdate},(err,data)=>{
        if(err){
            throw err
        }else{
            console.log(data)
        }
    })
    UpdateStatus.bulkSave()
}

const getMaxMin = async (address) =>{ 
    var query = {miner:address}
    var maxStamp, minStamp
    var maxResult, minResult
    var lastUpdate = new Date().getTime()
    maxResult=await Transaction.find(query).sort({timestamp:-1}).limit(1).exec()
    minResult=await Transaction.find(query).sort({timestamp:+1}).limit(1).exec()
    maxStamp = maxResult[0]['timestamp']
    minStamp = minResult[0]['timestamp']
    console.log(maxStamp,minStamp)
    createUpdateSta(address,maxStamp,minStamp)
}
const getMaxMinWorker = async (address) =>{ 
    var query = {address:address}
    var maxStamp, minStamp
    var maxResult, minResult
    var lastUpdate = new Date().getTime()
    maxResult=await Transaction.find(query).sort({timestamp:-1}).limit(1).exec()
    minResult=await Transaction.find(query).sort({timestamp:+1}).limit(1).exec()
    maxStamp = maxResult[0]['timestamp']
    minStamp = minResult[0]['timestamp']
    console.log(maxStamp,minStamp)
    createUpdateSta(address,maxStamp,minStamp)
}
const updateMaxMin = async (address) =>{ 
    var query = {miner:address}
    var maxStamp, minStamp
    var maxResult, minResult
    var lastUpdate = new Date().getTime()
    maxResult=await Transaction.find(query).sort({timestamp:-1}).limit(1).exec()
    minResult=await Transaction.find(query).sort({timestamp:+1}).limit(1).exec()
    maxStamp = maxResult[0]['timestamp']
    minStamp = minResult[0]['timestamp']
    console.log(maxStamp,minStamp)
    updateUpdateSta(address,maxStamp,minStamp)
}
// 日期格式转化为可读
function convertDateFormat(dateObj) {
    var s,d;
    d = new Date(0);
    d.setUTCSeconds(dateObj);
    // s = d.getUTCFullYear()+ "/";
    // s += d.getUTCMonth() + 1+ "/"; //months from 1-12
    // s += d.getUTCDate();
    return d;
}
//process transactions
function processTransaction(transaction){
    // console.log('processTransaction')
    transaction["value"]=parseInt(transaction["value"])/decimalBase
    var dateObj = new Date(0); // The 0 there is the key, which sets the date to the epoch
    dateObj.setUTCSeconds(transaction["timestamp"]);
    transaction.date= dateObj
    transaction.month = dateObj.getUTCMonth() + 1; //months from 1-12
    transaction.day = dateObj.getUTCDate();
    transaction.year = dateObj.getUTCFullYear();
    // console.log(transaction)
    // console.log('end of addDMYtoTransaction')
}
//insert document to a collection
function insertManyDocument(url,document, myObj){
    MongoClient.connect(url, function(err, db) {
      if (err) throw err;
      // var myobj = { name: "Company Inc", address: "Highway 37" };
      var dbo = db.db("filfox");
      dbo.collection(document).insertMany(myObj, function(err, res) {
        if (err) throw err;
        console.log("many document inserted");
        db.close();
      });
    });
  }
async function getTransWithTimestamp(address,maxTimestamp){
    // console.log('type ',type)
    var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=0`)
    var data = await res.data
    //save the first page data into mongodb
     //calculate the size of pages in total,
     var pageSize = parseInt(data["totalCount"]/100)
     var timestamp
     var continueLoop = true
    if( data["totalCount"] >0){
        //page start from 0
        for(var i=0;i<=pageSize;i++){
           if(continueLoop){
                   console.log("page",i)
               //i is page number
               res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${i}`)
               data = await res.data
               // console.log(data)
               for(var j=0;j<data["transfers"].length;j++){
                   //j is transaction number
                   timestamp = data["transfers"][j].timestamp
                   console.log('timestamp',timestamp)
                   if(timestamp > maxTimestamp){
                       console.log("transaction",j)
                       data["transfers"][j].miner=address
                       data["transfers"][j].address=address
                       processTransaction(data["transfers"][j])
                   }else{
                       continueLoop =false
                       data["transfers"].splice(j,1)

                   }
                }
             //insert the transaction of this page into db
             insertManyDocument(url,'transactions',data["transfers"])
           }else{
               //update maxtimestamp
               updateMaxMin(address)
               break;
           }
       }
            
    }
}
const getMaxMinAll = async()=>{
    const minerList=[
        'f092228',
        'f0130884',
        'f0112667',
        'f0155050',
        'f0402661',
        'f0672951',
        'f01602479',
        'f01606675',
        'f01606849',
        'f01641612',
        'f01238519',
        'f01264125',
        'f01466173',
        'f01694304',
        'f01731371',
        'f01776738',
        'f01372912',
        'f01479781',
        'f01662887',
        'f01716466',
        'f01716454',
    ]
    var maxStamp, maxResult
    // minStamp = minResult[0]['timestamp']
    for(const miner of minerList){
       getMaxMin(miner)
    }
}

const updateMaxMinAll = async()=>{
    const minerList=[
        'f092228',
        'f0130884',
        'f0112667',
        'f0155050',
        'f0402661',
        'f0672951',
        'f01602479',
        'f01606675',
        'f01606849',
        'f01641612',
        'f01238519',
        'f01264125',
        'f01466173',
        'f01694304',
        'f01731371',
        'f01776738',
        'f01372912',
        'f01479781',
        'f01662887',
        'f01716466',
        'f01716454',
    ]
    var maxStamp, maxResult
    // minStamp = minResult[0]['timestamp']
    for(const miner of minerList){
       updateMaxMin(miner)
    }
}

const syncOneAddressTransaction = async(address)=>{
    var query = {miner:address}
    var maxStamp
    var maxResult
    var lastUpdate = new Date().getTime()
    maxResult=await Transaction.find(query).sort({timestamp:-1}).limit(1).exec()
    maxStamp = maxResult[0]['timestamp']
    getTransWithTimestamp(address,maxStamp)
}
// getMaxMin()
// getMaxMinAll()
// updateMaxMinAll()
getMaxMinWorker('f3sxjuao74whiwmcnd7ad7hltkrbquzu7tp3rjqwlqa5mpngyvdsvo4x4g5hx5zl6xnvvoitewpwboagkacdmq')

