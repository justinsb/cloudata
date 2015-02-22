Repos = new Meteor.Collection("repos");


if (Meteor.isClient) {
Meteor.subscribe('repos', function() {
  console.log('Subscribed to repos');
});

	Template.body.helpers({
	  repos: function () {
		  return Repos.find();
	  }
	});
	
	// At the bottom of the client code
Accounts.ui.config({
  passwordSignupFields: "USERNAME_ONLY"
});
	
}