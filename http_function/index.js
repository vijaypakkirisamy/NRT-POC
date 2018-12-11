

exports.http_function = (request, response) => {

  // Imports the Cloud Datastore client library
  const Datastore = require('@google-cloud/datastore');

  // Your Google Cloud Platform project ID
  const projectId = process.env.GCLOUD_PROJECT;

  // Creates a client
  const datastore = new Datastore({
    projectId: projectId,
  });

  // Define the query: select the most recent record
  const query = datastore
    .createQuery('LimitBreaches')
    .order('datetime', {
      descending: true,
    })
    .limit(1);

  // Execute the query
	datastore.runQuery(query).then(results => {
    var responseString = '<html><head><meta http-equiv="refresh" content="5"></head><body>';
    const entities = results[0];

    if (entities.length && entities[0].hashtags.length) {
      responseString += '<h1>Limit Breaches</h1>';
      responseString += '<ol>';
      entities[0].hashtags.forEach(hashtag => {
        responseString += `<li>${hashtag.custidentifier} ${hashtag.custname} ${hashtag.txnamt} ${hashtag.location}`;
      });
      response.send(responseString);
    }
    else {
      responseString += 'No Limit Breaches at this time... Try again later.';
      response.send(responseString);
    }
  })
  .catch(err => {
    response.send(`Error: ${err}`);
  });
};