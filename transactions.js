const mongoose = require('mongoose');
const transactionSchema =new mongoose.Schema({
      _id: mongoose.Types.ObjectId,
      height: Number,
      address: String,
      timestamp: Number,
      from: String,
      to: String,
      value: Number,
      type: String,
      miner: String,
      date: String,
      day: Number,
      month: Number,
      year: Number,
      
     });

module.exports = mongoose.model('Transaction',transactionSchema)