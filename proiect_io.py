from redis import Redis
import time
import os
from datetime import datetime
from flask import request, jsonify, Response, Flask

app = Flask(__name__)
r = Redis(host=os.getenv('REDIS_HOST'), port=int(os.getenv('REDIS_PORT')), decode_responses=True)

r.set('admin_id', 0)
r.set('comp_id', 0)
r.set('emp_id', 0)
r.hset('admin', mapping={
    'id': 0,
	'name': 'admin'
})

def get_new_id(id_type: str) -> str:
	"""The function finds the next unused id and increments the global counter
	Args:
		id_type (str): The name of the type of id we want to get

	Returns:
		int: An unique ID
	"""

	new_id = id_type + '_' + str(r.incr(id_type))

	# If the id already exists, then we skip it
	while len(r.hgetall(new_id)) > 0:
		new_id = id_type + '_' + str(r.incr(id_type))

	return new_id


def decode_id(id: str) -> int:
	"""We keep the ids encoded, so that a city can have the same ID as a
		country for example, but in the database to have 2 separate keys.
		This function retrieves the number that is the ID.

	Args:
		id (str): The ID as it is in the database

	Returns:
		int: The actual ID of the entry
	"""

	return int(id.split('_')[2])


@app.route("/io/company", methods=["POST"])
def post_comp():
	payload = request.get_json()

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'name' in payload or not 'address' in payload or not 'email' \
 		or not 'comp_type' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['name'], str) or not isinstance(payload['email'], str) or \
		 not isinstance(payload['comp_type'], str) or not isinstance(payload['address'], str):

		return jsonify({'status': 'BAD REQUEST'}), 400

	# If the company already exists, then we return 409
	if r.sismember('comp', payload['name']) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	id = get_new_id('comp_id')
	r.sadd('comp', payload['name'])
	r.sadd('comp_ids', id)
	r.hset(id, mapping={
		'name': payload['name'],
		'address': payload['address'],
		'email': payload['email'],
		'comp_type': payload['comp_type']
	})

	return jsonify({'id': id}), 201


@app.route('/io/company/<string:company_id>', methods=['GET'])
def get_company(company_id):
    company_data = r.hgetall(company_id)
    if not company_data:
        return jsonify({'error': 'Company not found'}), 404
    return jsonify(company_data)


# Get all companies
@app.route('/io/company', methods=['GET'])
def get_all_companies():
    companies = []
    keys = r.smembers('comp_ids')
    for key in keys:
        companies.append(r.hgetall(key))
    return jsonify(companies)


# Update company by ID
@app.route('/io/company/<string:company_id>', methods=['PUT'])
def update_company(company_id):
    data = request.json
    company_data = r.hgetall(company_id)
    if not company_data:
        return jsonify({'error': 'Company not found'}), 404

    if 'name' in data:
        r.hset(company_id, 'name', data['name'])
    if 'address' in data:
        r.hset(company_id, 'address', data['address'])
    if 'email' in data:
        r.hset(company_id, 'email', data['email'])
    if 'comp_type' in data:
        r.hset(company_id, 'comp_type', data['comp_type'])

    return jsonify({'message': r.hgetall(company_id)})


# Delete company by ID
@app.route('/io/company/<string:company_id>', methods=['DELETE'])
def delete_company(company_id):
    if r.sismember('comp_ids', company_id):
        r.srem('comp', r.hget(company_id, 'name'))
        r.srem('comp_ids', company_id)
        r.hdel(company_id, 'name', 'address', 'email', 'comp_type')
        return jsonify({'message': 'Company deleted successfully'})

    return jsonify({'message': 'SUCCESS'}), 200

@app.route('/io/admin', methods=['POST'])
def create_admin():
    admin = request.json
    admin_id = get_new_id('admin_id')
    if r.exists(admin_id):
        return jsonify({'error': 'Admin already exists'}), 400
    r.sadd('admin_ids', admin_id)
    r.set(admin_id, admin['name'])
    return jsonify({'id': admin_id}), 201

@app.route('/io/admin/<string:id>', methods=['GET'])
def get_admin(id):
    admin = r.get(id)
    if not admin:
        return jsonify({'error': 'Admin not found'}), 404
    return jsonify(admin)

@app.route('/io/admin/<string:id>', methods=['PUT'])
def update_admin(id):
    admin = request.json
    if 'name' in admin:
        r.set(id, admin['name'])
    return jsonify({'result': 'success'})


@app.route('/io/admin/<string:id>', methods=['DELETE'])
def delete_admin(id):
    if not r.exists(id):
        return jsonify({'error': 'Admin not found'}), 404
    r.srem('admin_ids', id)  # Remove the admin id from the 'admins' set
    r.delete(id)
    return jsonify({'result': 'success'})

if __name__ == '__main__':
   app.run('0.0.0.0', debug=True)