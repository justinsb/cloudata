Repos = new Meteor.Collection("repos");


if (Meteor.isClient) {
	Meteor.subscribe('repos', function() {
	  console.log('Subscribed to repos');
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