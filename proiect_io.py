from redis import Redis
import time
import os
from datetime import datetime
from flask import request, jsonify, Response, Flask

app = Flask(__name__)
r = Redis(host='localhost', port=6379, decode_responses=True)

r.set('admin_id', 1)
r.set('comp_id', 0)
r.set('emp_id', 0)
r.hset('admin', mapping={
    'id': 0,
	'name': 'admin'
})

def get_new_id(id_type: str) -> int:
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
		 not isinstance(payload['comp_type'], str) or not isinstance(payload['address']):

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
		'comp_type': payload['comp_type'],
		'list_id_emp': []
	})

	return jsonify({'id': decode_id(id)}), 201


@app.route('/io/company/<int:company_id>', methods=['GET'])
def get_company(company_id):
    company_data = r.hgetall(f'company:{company_id}')
    if not company_data:
        return jsonify({'error': 'Company not found'}), 404
    return jsonify(company_data)

# Get all companies
@app.route('/company', methods=['GET'])
def get_all_companies():
    companies = []
    keys = db.keys('company:*')
    for key in keys:
        companies.append(db.hgetall(key))
    return jsonify(companies)

# Update company by ID
@app.route('/company/<int:company_id>', methods=['PUT'])
def update_company(company_id):
    data = request.json
    company_data = db.hgetall(f'company:{company_id}')
    if not company_data:
        return jsonify({'error': 'Company not found'}), 404
    db.hmset(f'company:{company_id}', data)
    return jsonify({'message': 'Company updated successfully'})

# Delete company by ID
@app.route('/company/<int:company_id>', methods=['DELETE'])
def delete_company(company_id):
    if db.exists(f'company:{company_id}'):
        db.delete(f'company:{company_id}')
        return jsonify({'message': 'Company deleted successfully'})
    else:
        return jsonify({'error': 'Company not found'}), 404

@app.route('/admin', methods=['POST'])
def create_admin():
    admin = request.json
    r.hmset('admin:' + admin['id'], admin)
    return jsonify({'result': 'success'}), 201

@app.route('/admin', methods=['GET'])
def get_all_admins():
    keys = r.keys('admin:*')
    admins = []
    for key in keys:
        admins.append(r.hgetall(key))
    return jsonify(admins)

@app.route('/admin/<id>', methods=['GET'])
def get_admin(id):
    admin = r.hgetall('admin:' + id)
    if not admin:
        return jsonify({'error': 'Admin not found'}), 404
    return jsonify(admin)

@app.route('/admin/<id>', methods=['PUT'])
def update_admin(id):
    admin = request.json
    r.hmset('admin:' + id, admin)
    return jsonify({'result': 'success'})

@app.route('/admin/<id>', methods=['DELETE'])
def delete_admin(id):
    r.delete('admin:' + id)
    return jsonify({'result': 'success'})

if __name__ == '__main__':
   app.run('0.0.0.0', debug=True)