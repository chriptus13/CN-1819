/**
* Responds to any HTTP request.
*
* @param {!express:Request} req HTTP request context.
* @param {!express:Response} res HTTP response context.
*/
var friends = [ 'joao', 'maria', 'jose','manel','zeca'];

exports.SayHello = (req, res) => {
	let maxnum=friends.length-1;
	if (req.query.num >= friends.length) {
		let err='I only know friends between 0 and '+maxnum;
		res.status(200).send(err);
	} else {
		let message='Dear '+friends[req.query.num]+', hello from '+req.query.myname;
		res.status(200).send(message);
	}
};