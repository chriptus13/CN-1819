const vision = require('@google-cloud/vision');
const client = new vision.ImageAnnotatorClient();

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload and metadata.
 * @param {!Function} callback Callback function to signal completion.
 */
exports.AnalyzeImage = (event, callback) => {
  const file = 'gs://'+event.data.bucket+'/'+event.data.name;
  console.log(`Analyzing ${file}`);

  // https://cloud.google.com/nodejs/docs/reference/vision/0.19.x/google.cloud.vision.v1.html#.AnnotateImageResponse
  client.labelDetection(file).then(results => {
    const labels = results[0].labelAnnotations;
    console.log('Labels:');
    labels.forEach(label => console.log(label.description + ' / ' + label.score));
  })
  .catch(err => {
    console.error('Error: ', err);
  });  
  callback();
};