Router.route('/', function () {
  this.render('HomePage');
});

Router.route('/repo/:_owner/:_repo', function () {
  var repoName = this.params._owner + '/' + this.params._repo;
  var sha = this.params._sha;
  
  this.wait(Meteor.subscribe('repos', repoName));
  this.wait(Meteor.subscribe('revlogs',  repoName));
  this.wait(Meteor.subscribe('files', repoName));

  if (this.ready()) {
  	var data = {};
    data.revlogs = RevLogs.find({ });
    data.repo = Repos.findOne({ name: repoName });
    data.files = Files.find({ repo: repoName });
    
    this.render('RepoPage', {data: data});
  } else {
    //this.render('Loading');
  }
  
});


Router.route('/repo/:_owner/:_repo/commit/:_sha', function () {
  var repoName = this.params._owner + '/' + this.params._repo;
  var sha = this.params._sha;
  
  this.wait(Meteor.subscribe('repos', repoName));
  this.wait(Meteor.subscribe('diffs', repoName, sha));

  if (this.ready()) {
	var diffs = Diffs.find({ sha: sha, repo: repoName });
    var repo = Repos.findOne({ name: repoName });
    this.render('DiffPage', {data: { diffs: diffs, repo: repo }});
  } else {
    //this.render('Loading');
  }
  
});


Router.route('/repo/:_owner/:_repo/tree/master/:_path(.*)', function () {
  var repoName = this.params._owner + '/' + this.params._repo;
  var path = this.params._path;
  
  this.wait(Meteor.subscribe('repos', repoName));
  this.wait(Meteor.subscribe('filecontents', repoName, path));

  if (this.ready()) {
  var data = {};
    data.repo = Repos.findOne({ name: repoName });
    data.filecontent = FileContents.findOne({ repo: repoName, path: path });
    this.render('FileContentsPage', {data: data});
  } else {
    //this.render('Loading');
  }
  
});

