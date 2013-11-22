define([ 'angular', 'app'], function(angular, app) {
	'use strict';

	return app.config([ '$routeProvider', function($routeProvider) {

		$routeProvider.
			when('/overview', {templateUrl: 'partials/overview.html', controller: 'OverviewCtrl'}).
			when('/collections', {templateUrl: 'partials/collections.html', controller: 'CollectionsListCtrl'}).
			when('/deliveries', {templateUrl: 'partials/deliveries.html', controller: 'DeliveriesListCtrl'}).
			when('/browse', {templateUrl: 'partials/browse.html', controller: 'BrowseCtrl'}).
			when('/replication', {templateUrl: 'partials/replication.html', controller: 'ReplicationCtrl'}).
			when('/monitoring', {templateUrl: 'partials/monitoring/monitoring.html', controller: 'MonitoringCtrl'}).
			when('/help', {templateUrl: 'partials/help.html', controller: 'HelpCtrl'}).
			otherwise({redirectTo: '/overview'});
	} ]);

});

