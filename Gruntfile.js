'use strict';

module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    benchmark: {
        all: {
            src: ['benchmarks/*.js'],
            dest: 'benchmarks.csv'
        }
    },
    yuidoc: {
        compile: {
            name: '<%= pkg.name %>',
            description: '<%= pkg.description %>',
            version: '<%= pkg.version %>',
            url: '<%= pkg.homepage %>',
            options: {
                paths: './lib/',
                outdir: './docs/',
                tabtospace: 4
            }
        }
    },
    jshint: {
      files: ['Gruntfile.js', 'lib/**/*.js', 'test/**/*.js'],
      options: {
        node: true,
        mocha: true
      }
    },
    mochaTest: {
        unit: {
            options: {
                reporter: 'spec',
                captureFile: 'unit-test-results.txt',
                quiet: false,
                clearRequireCache: false,
                colors: false
            },
            src: ['test/unit/**/*.js']
        },
        integration: {
            options: {
                reporter: 'spec',
                captureFile: 'integration-test-results.txt',
                quiet: false,
                clearRequireCache: false,
                colors: false
            },
            src: ['test/integration/core/*.js', 'test/integration/crdt/*.js', 'test/integration/kv/*.js', 'test/integration/mapreduce/*.js', 'test/integration/yokozuna/*.js']
        },
        integration_hll: {
            options: {
                reporter: 'spec',
                captureFile: 'integration-test-results.txt',
                quiet: false,
                clearRequireCache: false,
                colors: false
            },
            src: ['test/integration/dt/hll.js']
        },
        timeseries: {
            options: {
                reporter: 'spec',
                captureFile: 'integration-test-results.txt',
                quiet: false,
                clearRequireCache: false,
                colors: false
            },
            src: ['test/unit/ts/*.js', 'test/integration/ts/*.js']
        },
        security: {
            options: {
                reporter: 'spec',
                captureFile: 'security-test-results.txt',
                quiet: false,
                clearRequireCache: false
            },
            src: ['test/security/**/*.js']
        }
    },
    watch: {
      files: ['<%= jshint.files %>'],
      tasks: ['jshint']
    }
  });

  var enable_debug = process.env.RIAK_NODEJS_CLIENT_DEBUG ||
    process.env.GRUNT_DEBUG || grunt.option('debug');
  if (enable_debug) {
    grunt.log.write('enabling test debugging in Mocha');
    grunt.config.set('mochaTest.unit.options.require', 'test/debug-log');
    grunt.config.set('mochaTest.integration.options.require', 'test/debug-log');
    grunt.config.set('mochaTest.security.options.require', 'test/debug-log');
  } else {
    grunt.config.set('mochaTest.options.require', 'test/disable-log');
  }

  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-yuidoc');
  grunt.loadNpmTasks('grunt-benchmark');

  grunt.registerTask('lint', 'jshint');
  grunt.registerTask('unit', function (testSuite) {
    if (testSuite) {
        testSuite = testSuite.trim();
        if (testSuite !== '') {
            grunt.config.set('mochaTest.unit.src', ['test/unit/' + testSuite + '/*.js']);
        }
    }
    grunt.task.run(['jshint', 'mochaTest:unit']);
  });
  grunt.registerTask('integration', ['jshint', 'mochaTest:integration']);
  grunt.registerTask('integration-hll', ['jshint', 'mochaTest:integration_hll']);
  grunt.registerTask('timeseries', ['jshint', 'mochaTest:timeseries']);
  grunt.registerTask('security-test', ['mochaTest:security']);
  grunt.registerTask('security', ['jshint', 'mochaTest:security']);
  grunt.registerTask('default', ['jshint', 'mochaTest:unit', 'mochaTest:integration']);
  grunt.registerTask('docs', 'yuidoc');

};
