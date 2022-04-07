"use strict";
/*!
 * Simple library to send files over WebRTC
 *
 * @author   Subin Siby <https://subinsb.com>
 * @license  MPL-2.0
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.PeerFileReceive = exports.PeerFileSend = void 0;
var Peer = require("simple-peer");
var PeerFileSend_1 = require("./PeerFileSend");
exports.PeerFileSend = PeerFileSend_1.default;
var PeerFileReceive_1 = require("./PeerFileReceive");
exports.PeerFileReceive = PeerFileReceive_1.default;
var SimplePeerFiles = /** @class */ (function () {
    function SimplePeerFiles() {
        this.arrivals = {};
    }
	
    SimplePeerFiles.prototype.send = function (peer, fileID, file, wrtc) {
        return new Promise(function (resolve) {
            var controlChannel = peer;
            var startingByte = 0;
			
            var fileChannel = new Peer({
                initiator: true,
				//reconnectTimer: 500,
				iceTransportPolicy: 'relay',
				trickle: false,
				config: {
					iceServers: [
                        {
                            urls: ['stun:rocketchat:3478', 'turn:rocketchat:3478'],
                            username: "rocketchat",
                            credential: "rocketchat"
                        }]
				},
				wrtc: wrtc
            });

            fileChannel.destroy=function(){
                try{
                    fileChannel._destroy(null, () => {})
                }
                catch(e){
                    console.log('error while destroy fileChannel:', e);
                }
            }

            fileChannel.on('signal', function (signal) {
                try{
                    controlChannel.send(JSON.stringify({
                        fileID: fileID,
                        signal: signal
                    }));
                }
                catch(e){
                    console.log('error at fileChannel signal:', e);
                }
            });
            var controlDataHandler = function (data) {
                try {
                    var dataJSON = JSON.parse(data);
                    
                    if (dataJSON.signal && dataJSON.fileID && dataJSON.fileID === fileID) {
                        if (dataJSON.start) {
                            startingByte = dataJSON.start;
                        }
                        fileChannel.signal(dataJSON.signal);
                    }
                }
                catch (e) { }
            };
            fileChannel.on('connect', function () {
                var pfs = new PeerFileSend_1.default(fileChannel, file, startingByte);
                var destroyed = false;
                var destroy = function () {
                    if (destroyed)
                        return;
                    controlChannel.removeListener('data', controlDataHandler);
                    
                    setTimeout(function(){
                        fileChannel.destroy();
                        // garbage collect
                        controlDataHandler = null;
                        pfs = null;
                        destroyed = true;
                    }, 1000);
                };
                pfs.on('done', destroy);
                pfs.on('cancel', destroy);
                fileChannel.on('close', function () {
                    if(pfs!=null)pfs.cancel();
                });
                resolve(pfs);
            });

            controlChannel.on('data', controlDataHandler);
        });
    };
    SimplePeerFiles.prototype.receive = function (peer, fileID, wrtc) {
        var _this = this;
        return new Promise(function (resolve) {
            var controlChannel = peer;
            var fileChannel = new Peer({
                initiator: false,
				//reconnectTimer: 500,
				iceTransportPolicy: 'relay',
				trickle: false,
				config: {
					iceServers:[
                        {
                            urls: ['stun:rocketchat:3478', 'turn:rocketchat:3478'],
                            username: "rocketchat",
                            credential: "rocketchat"
                        }]
				},
				wrtc: wrtc
            });

            fileChannel.destroy= function(){
                try{
                    fileChannel._destroy(null, () => {})
                }
                catch(e){
                    console.log('error while destroy fileChannel:', e);
                }
            }

            fileChannel.on('signal', function (signal) {
				try{
					// chunk to start sending from
					var start = 0;
					// File resume capability
					if (fileID in _this.arrivals) {
						start = _this.arrivals[fileID].bytesReceived;
					}
					controlChannel.send(JSON.stringify({
						fileID: fileID,
						start: start,
						signal: signal
					}));
				}
				catch(e){
					console.log(e);
				}
            });
            var controlDataHandler = function (data) {
                try {
                    var dataJSON = JSON.parse(data);
                    if (dataJSON.signal && dataJSON.fileID && dataJSON.fileID === fileID) {
                        fileChannel.signal(dataJSON.signal);
                    }
                }
                catch (e) { }
            };
            fileChannel.on('connect', function () {
				try{
					var pfs;
					if (fileID in _this.arrivals) {
						pfs = _this.arrivals[fileID];
						pfs.setPeer(fileChannel);
					}
					else {
						pfs = new PeerFileReceive_1.default(fileChannel);
						_this.arrivals[fileID] = pfs;
					}
					var destroyed = false;
					var destroy = function () {
						if (destroyed)
							return;
						controlChannel.removeListener('data', controlDataHandler);
						delete _this.arrivals[fileID];

						setTimeout(function(){
							fileChannel.destroy();
							// garbage collect
							controlDataHandler = null;
							pfs = null;
							destroyed = true;
						}, 1000);
					};
					pfs.on('done', destroy);
					pfs.on('cancel', destroy);
					resolve(pfs);
				}
				catch(e){
					console.log(e);
				}
            });
            controlChannel.on('data', controlDataHandler);

        });
    };
    return SimplePeerFiles;
}());
exports.default = SimplePeerFiles;
