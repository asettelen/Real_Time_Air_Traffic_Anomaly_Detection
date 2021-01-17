var planeCount = 0;
var radarCount = 0;

var guiEnabled = true;

function enableGUI(reportEnabled) {
	if(!guiEnabled) {
		guiEnabled = true;
		controls = document.getElementsByClassName('control');
		for(var i = 0; i < controls.length; i++) {
			controls[i].disabled = false;
		}
		if(!reportEnabled) {
			document.getElementById('btnMakeReportType1').disabled = true;
			document.getElementById('btnMakeReportType2').disabled = true;
		}
		document.getElementById('loading').style.display = 'none';
	}
}

function disableGUI() {
	if(guiEnabled) {
		guiEnabled = false;
		controls = document.getElementsByClassName('control');
		for(var i = 0; i < controls.length; i++) {
			controls[i].disabled = true;
		}
		document.getElementById('loading').style.display = 'inline-block';
	}
}

function startBatch() {
	disableGUI();
	args = {};
	args['action'] = 'startBatch';
	args['startDate'] = new Date(document.getElementById('startDate').value).getTime();
	args['endDate'] = new Date(document.getElementById('endDate').value).getTime();
	callback = function(response) {
		enableGUI(true);
		response = response.split('|');
		planes = response[0].split(',');
		radars = response[1].split(',');
		planeCount = planes.length;
		radarCount = radars.length;
		content = '';
		for(var i = 0; i < radars.length; i++) {			
			content += '<div><input class="control" type="checkbox" id="radar' + i + '" value="' + radars[i] + '" /><label for="radar' + i + '">' + radars[i] + '</label><br /></div>';
		}
		document.getElementById('radarList').innerHTML = content;
		content = '';
		for(var i = 0; i < planes.length; i++) {			
			content += '<div><input class="control" type="checkbox" id="plane' + i + '" value="' + planes[i] + '" /><label for="plane' + i + '">' + planes[i] + '</label><br /></div>';
		}
		document.getElementById('planeList').innerHTML = content;
	}
	xhrGET("http://192.168.37.152/viz/", args, callback);
}

function startRealtime() {
	
}

function stop() {
	disableGUI();
	args = {};
	args['action'] = 'stop';
	callback = function(response) {
		enableGUI(false);
	}
	xhrGET("http://192.168.37.152/viz/", args, callback);	
}

function makeReport(type) {
	var planes = '';
	var radars = '';
	for(var i = 0; i < planeCount; i++) {
		var checkbox = document.getElementById('plane' + i);
		if(checkbox.checked) {
			planes += checkbox.value + ',';
		}
	}
	if(planes.length > 0) {
		planes = planes.substr(0, planes.length - 1);
	}
	for(var i = 0; i < radarCount; i++) {
		var checkbox = document.getElementById('radar' + i);
		if(checkbox.checked) {
			radars += checkbox.value + ',';
		}
	}
	if(radars.length > 0) {
		radars = radars.substr(0, radars.length - 1);
	}
	disableGUI();
	args = {};
	args['action'] = 'makeReport';
	args['type'] = type;
	args['filterPlane'] = planes;
	args['filterRadar'] = radars;
	callback = function(response) {
		enableGUI(true);
		window.open(response, '_blank');
	}
	xhrGET("http://192.168.37.152/viz/", args, callback);
}

function xhrGET(url, args, callback) {
	url += "?";
	for(k in args) {
		url += k + "=" + encodeURIComponent(args[k]) + "&";
	}
	url = url.substr(0, url.length - 1);
	var xhr = new XMLHttpRequest();
	xhr.open("GET", url, true);
	xhr.onload = function(e) {
		if (xhr.readyState === 4) {
			if (xhr.status === 200) {
				console.log(xhr.responseText);
				callback(xhr.responseText);
			} else {
				console.error(xhr.statusText);
			}
		}
	};
	xhr.onerror = function (e) {
		console.error(xhr.statusText);
	};
	xhr.send(null); 
}