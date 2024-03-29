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
r.set('book_id', 0)
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
	return jsonify({
		'id': company_id,
  		'name': company_data['name'],
		'address': company_data['address'],
		'email': company_data['email'],
		'comp_type': company_data['comp_type']
	})


# Get all companies
@app.route('/io/company', methods=['GET'])
def get_all_companies():
	companies = []
	keys = r.smembers('comp_ids')
	for key in keys:
		company_data = r.hgetall(key)
		companies.append({
			'id': key,
			'name': company_data['name'],
			'address': company_data['address'],
			'email': company_data['email'],
			'comp_type': company_data['comp_type']
		})
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
	return jsonify({
		'id': id,
		'name': admin['name']
	})


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


@app.route('/io/employee', methods=['POST'])
def create_employee():
	payload = request.get_json()

	if not 'first_name' in payload  \
 		or not 'last_name' in payload or not 'email' \
 		or not 'email' in payload or not 'phone_number' \
        in payload or not 'id_comp' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	company_data = r.hgetall(payload['id_comp'])
	if not company_data:
		return jsonify({'error': 'Company not found'}), 404

	id = get_new_id('emp_id')
	r.sadd('emp_ids', id)
	r.hset(id, mapping={
		'first_name': payload['first_name'],
		'last_name': payload['last_name'],
		'email': payload['email'],
		'phone_number': payload['phone_number'],
		'id_comp': payload['id_comp']
	})

	return jsonify({'id': id}), 201


@app.route('/io/employee/<string:id>', methods=['GET'])
def get_employee(id):
	employee_data = r.hgetall(id)
	if not employee_data:
		return jsonify({'error': 'Employee not found'}), 404
	return jsonify({
		'id': id,
		'first_name': employee_data['first_name'],
		'last_name': employee_data['last_name'],
		'email': employee_data['email'],
		'phone_number': employee_data['phone_number'],
		'id_comp': employee_data['id_comp']
	})


@app.route('/io/employee', methods=['GET'])
def get_all_employees():
    keys = r.smembers('emp_ids')
    employees = []
    for key in keys:
        employee_data = r.hgetall(key)
        employees.append({
			'id': key,
			'first_name': employee_data['first_name'],
			'last_name': employee_data['last_name'],
			'email': employee_data['email'],
			'phone_number': employee_data['phone_number'],
			'id_comp': employee_data['id_comp']
		})
    return jsonify(employees)


@app.route('/io/employee/<string:id>', methods=['PUT'])
def update_employee(id):
	data = request.json
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	if 'id_comp' in data:
		company_data = r.hgetall(data['id_comp'])
		if not company_data:
			return jsonify({'error': 'Company not found'}), 404

	if 'name' in data:
		r.hset(id, 'name', data['name'])
	if 'address' in data:
		r.hset(id, 'address', data['address'])
	if 'email' in data:
		r.hset(id, 'email', data['email'])
	if 'phone_number' in data:
		r.hset(id, 'phone_number', data['phone_number'])
	if 'id_comp' in data:
		r.hset(id, 'id_comp', data['id_comp'])

	return jsonify({'message': r.hgetall(id)})


@app.route('/io/employee/<string:id>', methods=['DELETE'])
def delete_employee(id):
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404
	r.delete(id)
	return jsonify({'message': 'Employee deleted successfully'})


@app.route('/io/employee/<string:id>/books/active', methods=['POST'])
def add_active_book(id):
    book_id = request.json.get('book_id')
    if not r.exists(f'employee:{id}'):
        return jsonify({'error': 'Employee not found'}), 404

    r.sadd(f'employee:{id}:books:active', book_id)
    return jsonify({'message': 'Book added to active list successfully'})

@app.route('/io/employee/<string:id>/books/wishlist', methods=['POST'])
def add_wishlist_book(id):
    book_id = request.json.get('book_id')
    if not r.exists(f'employee:{id}'):
        return jsonify({'error': 'Employee not found'}), 404

    r.sadd(f'employee:{id}:books:wishlist', book_id)
    return jsonify({'message': 'Book added to wishlist successfully'})


@app.route('/io/employee/<string:id>/books/listened', methods=['POST'])
def add_listened_book(id):
    book_id = request.json.get('book_id')
    if not r.exists(f'employee:{id}'):
        return jsonify({'error': 'Employee not found'}), 404

    r.sadd(f'employee:{id}:books:listened', book_id)
    return jsonify({'message': 'Book added to listened list successfully'})


@app.route('/io/employee/<string:id>/books', methods=['GET'])
def get_employee_books(id):
    if not r.exists(f'employee:{id}'):
        return jsonify({'error': 'Employee not found'}), 404

    active_books = list(r.smembers(f'employee:{id}:books:active'))
    wishlist_books = list(r.smembers(f'employee:{id}:books:wishlist'))
    listened_books = list(r.smembers(f'employee:{id}:books:listened'))
    return jsonify({
        'active_books': active_books,
        'wishlist_books': wishlist_books,
        'listened_books': listened_books
    })


if __name__ == '__main__':
   app.run('0.0.0.0', debug=True)