define([
	'angular',
	'controllers',
	'angularUiBootstrap',
	'angularUiBootstrapTpls'
], function (angular, controllers) {
	'use strict';

	// Declare app level module which depends on controllers
	return angular.module('jumbodb', [
		'ui.bootstrap',
		'jumbodb.controllers'

	]);
});




