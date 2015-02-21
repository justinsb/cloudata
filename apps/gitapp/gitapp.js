DataConnection = DDP.connect('http://127.0.0.1:8888/');

Repos = new Meteor.Collection("repos", DataConnection);


if (Meteor.isClient) {
DataConnection.subscribe('repos', function() {
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