define([
	'angular',
	'controllers',
	'angularUiBootstrap',
	'angularUiBootstrapTpls',
	'jsonExplorer'
], function (angular, controllers) {
	'use strict';

	// Declare app level module which depends on controllers
	return angular.module('jumbodb', [
		'ui.bootstrap',
        'gd.ui.jsonexplorer',
        'jumbodb.controllers'

	]);
});




