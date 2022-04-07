"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
var ee_ts_1 = require("ee-ts");
var readable_stream_1 = require("readable-stream");

var Meta_1 = require("./Meta");
var CHUNK_SIZE = Math.pow(2, 16)-1;
/**
 * Make a Uint8Array to send to peer
 * @param header Type of data. See Meta.ts
 * @param data
 */
//var from2 = require('from2')

function pMsg(header, data) {
    if (data === void 0) { data = null; }
    var resp;
    if (data) {
        resp = new Uint8Array(1 + data.length);
        resp.set(data, 1);
    }
    else {
        resp = new Uint8Array(1);
    }
    resp[0] = header;
    return resp;
}
var SendStream = /** @class */ (function (_super) {
    __extends(SendStream, _super);
    function SendStream(fileSize, bytesSent) {
        if (bytesSent === void 0) { bytesSent = 0; }
        var _this = _super.call(this) || this;
        _this.bytesSent = 0;
        _this.fileSize = 0; // file size
        _this.paused = false;
        _this.fileSize = fileSize;
        _this.bytesSent = bytesSent;
        return _this;
    }
    SendStream.prototype._read = function () {
        if (this.cb)
            this.cb(null);
    };
    /**
     * File stream writes here
     * @param chunk
     * @param encoding
     * @param cb
     */
    SendStream.prototype._write = function (chunk, encoding, cb) {
        try{
            if (this.paused)
                return;
            // Will return true if additional chunks of data may continue to be pushed
            var availableForMore = this.push(pMsg(Meta_1.ControlHeaders.FILE_CHUNK, chunk));
            this.bytesSent += chunk.byteLength;
            var percentage = parseFloat((100 * (this.bytesSent / this.fileSize)).toFixed(3));
            this.emit('progress', percentage, this.bytesSent);
            if (availableForMore) {
                this.cb = null;
                cb(null); // Signal that we're ready for more data
            }
            else {
                this.cb = cb;
            }
        }
        catch(e){
            console.log('SendStream.prototype._write error:', e);
        }
    };
    return SendStream;
}(readable_stream_1.Duplex));
var PeerFileSend = /** @class */ (function (_super) {
    __extends(PeerFileSend, _super);
    /**
     * @param peer   Peer to send
     * @param file   File to send
     * @param offset Bytes to start sending from, useful for file resume
     */
    function PeerFileSend(peer, file, offset) {
        if (offset === void 0) { offset = 0; }
        var _this = _super.call(this) || this;
        _this.paused = false;
        _this.cancelled = false;
        _this.receiverPaused = false;
        // Bytes to start sending from
        _this.offset = 0;
        _this.peer = peer;
        _this.file = file;
        _this.offset = offset;
        return _this;
    }
    /**
     * Send a message to receiver
     * @param header Type of message
     * @param data   Message
     */
    PeerFileSend.prototype.sendPeer = function (header, data) {
		
        if (data === void 0) { data = null; }
        if (!this.peer.connected)
            return;

        this.peer.send(pMsg(header, data));
    };
    // Info about file is sent first
    PeerFileSend.prototype.sendFileStartData = function () {
	
        var meta = {
            fileName: this.file.name,
            fileSize: this.file.size,
            fileType: this.file.type
        };
        var metaString = JSON.stringify(meta);
        var metaByteArray = new TextEncoder().encode(metaString);
        this.sendPeer(Meta_1.ControlHeaders.FILE_START, metaByteArray);
    };
    PeerFileSend.prototype.setPeer = function (peer) {
        this.peer = peer;
    };
	
	
	/*var from = require('from2');
	function fromBuffer(buf) {
	  return from(function(size, next) {
		// if there's no more content
		// left in the string, close the stream.
		if (buf.length <= 0){
			return next(null, null)
		}
		// Pull in a new chunk of text,
		// removing it from the string.
		var chunk = buf.slice(0, CHUNK_SIZE)
		buf = buf.slice(CHUNK_SIZE)
	 
		// Emit "chunk" from the stream.
		next(null, chunk)
	  })
	}*/
	
    // Start sending file to receiver
    PeerFileSend.prototype._resume = function () {
	
        var _this = this;
        if (this.receiverPaused)
            return;
        if (this.offset===0) {
            // Start
            this.sendFileStartData();
            this.emit('progress', 0.0, 0);			
        }
		
		const fs=require('fs');
		const readable=fs.createReadStream(this.file.path, {highWaterMark: CHUNK_SIZE, start: this.offset});
		this.ss = new SendStream(/*fs.statSync(this.file.path).size*/this.fileSize, this.offset);
		this.ss.on('progress', function (percentage, bytes) {
			_this.emit('progress', percentage, bytes);
		});
		
		readable.pipe(this.ss).pipe(this.peer);
		
    };
    PeerFileSend.prototype.start = function () {

        var _this = this;
        // Listen for cancel requests
        this.peer.on('data', function (data) {

            if (data[0] === Meta_1.ControlHeaders.FILE_END) {
                _this.emit('progress', 100.0, _this.file.size);
                _this.emit('done');
            }
            else if (data[0] === Meta_1.ControlHeaders.TRANSFER_PAUSE) {
                _this._pause();
                _this.receiverPaused = true;
                _this.emit('paused');
            }
            else if (data[0] === Meta_1.ControlHeaders.TRANSFER_RESUME) {
                _this.receiverPaused = false;
				_this.emit('resumed_remote');
				
                if (!_this.paused) {
                    _this._resume();
                    _this.emit('resumed');
                }
            }
            else if (data[0] === Meta_1.ControlHeaders.TRANSFER_CANCEL) {
                _this.cancelled = true;
                _this.peer.destroy();
                _this.emit('cancelled');
            }
        });
        this._resume();
    };
    // Pause transfer and store the bytes sent till now for resuming later
    PeerFileSend.prototype._pause = function () {
        this.ss.paused = true;
        this.offset = this.ss.bytesSent;
    };
    // Stop sending data now & future sending
    PeerFileSend.prototype.pause = function () {
        this._pause();
        this.paused = true;
        this.sendPeer(Meta_1.ControlHeaders.TRANSFER_PAUSE);
        this.emit('pause');
    };
    // Allow data to be sent & start sending data
    PeerFileSend.prototype.resume = function () {
        this.paused = false;
        this._resume();
		this.sendPeer(Meta_1.ControlHeaders.TRANSFER_RESUME);
        this.emit('resume');
    };
    PeerFileSend.prototype.cancel = function () {
        /*this.cancelled = true;
        this.ss.destroy();
        this.sendPeer(Meta_1.ControlHeaders.TRANSFER_CANCEL);
        this.peer.destroy();
        this.emit('cancel');*/
		try{
            this.cancelled = true;
            this.sendPeer(Meta_1.ControlHeaders.TRANSFER_CANCEL);

            setTimeout(function(){
                if(this.ss)this.ss.destroy();
                if(this.peer)this.peer.destroy();
            }, 1000);
            
            this.emit('cancel');
        }
        catch(e){
            console.log('error on PeerFileSend.prototype.cancel:', e);
        }
    };
    return PeerFileSend;
}(ee_ts_1.EventEmitter));
exports.default = PeerFileSend;
