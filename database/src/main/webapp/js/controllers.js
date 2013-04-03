function OverviewCtrl($scope, $http) {
    $http.get('/jumbodb/rest/status').success(function(data) {
        $scope.status = data;
    });

}

function CollectionsListCtrl($scope) {

}

function DeliveriesListCtrl($scope) {

}