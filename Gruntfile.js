module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    yuidoc: {
        compile: {
            name: '<%= pkg.name %>',
            description: '<%= pkg.description %>',
            version: '<%= pkg.version %>',
            url: '<%= pkg.homepage %>',
            options: {
                paths: './lib/',
                outdir: './docs/'
            }
        }
    },
    jshint: {
      files: ['Gruntfile.js', 'lib/**/*.js', 'test/**/*.js'],
    },
    mochaTest: {
        unit: {
            options: {
                reporter: 'spec',
                captureFile: 'unit-test-results.txt',
                quiet: false,
                clearRequireCache: false
            },
            src: ['test/unit/**/*.js']
        },
        integration: {
            options: {
                reporter: 'spec',
                captureFile: 'integration-test-results.txt',
                quiet: false,
                clearRequireCache: false
            },
            src: ['test/integration/**/*.js']
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

  if (grunt.option('debug') || process.env.GRUNT_DEBUG) {
    grunt.log.write('enabling test debugging in Mocha');
    grunt.config.set('mochaTest.unit.options.require', 'test/debug-log');
    grunt.config.set('mochaTest.integration.options.require', 'test/debug-log');
    grunt.config.set('mochaTest.security.options.require', 'test/debug-log');
  }

  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-yuidoc');

  grunt.registerTask('lint', 'jshint');
  grunt.registerTask('unit', 'mochaTest:unit');
  grunt.registerTask('integration', 'mochaTest:integration');
  grunt.registerTask('security', 'mochaTest:security');
  grunt.registerTask('default', ['jshint', 'mochaTest:unit', 'mochaTest:integration']);
  grunt.registerTask('docs', 'yuidoc');

};
