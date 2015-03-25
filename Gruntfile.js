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
        }
    },   
    watch: {
      files: ['<%= jshint.files %>'],
      tasks: ['jshint']
    }
  });

  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-yuidoc');

  grunt.registerTask('default', ['jshint', 'mochaTest']);
  grunt.registerTask('unit', 'mochaTest:unit');
  grunt.registerTask('integration', 'mochaTest:integration');
  grunt.registerTask('docs', ['yuidoc']);

};
