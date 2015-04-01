Repos = new Meteor.Collection("repos");
RevLogs = new Meteor.Collection("revlogs");
Diffs = new Meteor.Collection("diffs");
Files = new Meteor.Collection("files");
FileContents = new Meteor.Collection("filecontents");


if (Meteor.isClient) {
	Meteor.subscribe('repos', function() {
	  console.log('Subscribed to repos');
	});
	
	Template.registerHelper("highlight", function (src) {
		var result = hljs.highlightAuto(src, ['java','xml']);
		return result.value;	
	});
	

	Template.HomePage.helpers({
	  repos: function () {
		  return Repos.find({}, { sort: [ 'name' ] });
	  },
	});
	
	Template.repo.helpers({
	  url: function () {
	    var repo = this;
	    return encodeURI(repo.name);
	  }
	});
	
	// At the bottom of the client code
	Accounts.ui.config({
	  passwordSignupFields: "USERNAME_ONLY"
	});
}