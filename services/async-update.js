let config = require('config');
let redis = require('redis');
let db = require('../lib/db');
let log = require('npmlog');

let updateDeliveredCampaigns = (campaignId, status, response, responseId, messageId) => {
    db.getConnection((err, connection) => {
        if (err) {
            log.error('Mail', err.stack);
            return;
        }

        let query = 'UPDATE `campaign__' + campaignId + '` SET status=?, response=?, response_id=?, updated=NOW() WHERE id=? LIMIT 1';

        connection.query(query, [status, response, responseId, messageId], err => {
            connection.release();
            if (err) {
                log.error('Mail', err.stack);
            } else {
                // log.verbose('Mail', 'Message sent and status updated for %s', message.subscription.cid);
            }
        });
    });
}


let updateBlacklistedCampaigns = (campaignId, messageId) => {
    db.getConnection((err, connection) => {
        if (err) {
            log.error('Mail', err);
            return;
        }

        let query = 'UPDATE `campaign__' + campaignId + '` SET status=?, response=?, response_id=?, updated=NOW() WHERE id=? LIMIT 1';

        connection.query(query, [5, 'blacklisted', 'blacklisted', messageId], err => {
            connection.release();
            if (err) {
                log.error('Mail', err);
            }
        });
    });
}

if (config.redis && config.redis.enabled) {
    var blpopQueue = function() {
      client.blpop('dataQueue', 0, function(err, _data){
        // {status: 3, campaignId: 35, response: "123@123.com", responseId: "123@123.com", messageId: 1}
        let data = JSON.parse(_data[1])
        if(data.action == 'delivered'){
            updateDeliveredCampaigns(data.campaignId, data.status, data.response, data.responseId, data.messageId)
        }else if(data.action == 'blacklisted'){
            updateBlacklistedCampaigns(data.campaignId, data.messageId)
        }
        blpopQueue();
      });
    };
    const client = redis.createClient(config.redis.port, config.redis.host);
    log.info("async update start...")
    blpopQueue()
}
