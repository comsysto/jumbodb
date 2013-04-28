angular.module('jumbodb', []).
    config(['$routeProvider', function ($routeProvider) {
        $routeProvider.
            when('/overview', {templateUrl: 'partials/overview.html', controller: OverviewCtrl}).
            when('/collections', {templateUrl: 'partials/collections.html', controller: CollectionsListCtrl}).
            when('/deliveries', {templateUrl: 'partials/deliveries.html', controller: DeliveriesListCtrl}).
            when('/browse', {templateUrl: 'partials/browse.html', controller: BrowseCtrl}).
            when('/replication', {templateUrl: 'partials/replication.html', controller: ReplicationCtrl}).
//            when('/phones/:phoneId', {templateUrl: 'partials/phone-detail.html', controller: PhoneDetailCtrl}).
            otherwise({redirectTo: '/overview'});
    }]);