from redis import Redis
import time
import os
from datetime import datetime
from flask import request, jsonify, Response, Flask

app = Flask(__name__)
r = Redis(host=os.getenv('REDIS_HOST'), port=int(os.getenv('REDIS_PORT')), decode_responses=True)

r.set('comp_id', 0)
r.set('emp_id', 0)
r.set('book_id', 0)
r.hset('admin', mapping={
	'id': 'admin_id_1',
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

	# If the company already exists, then we return 409
	if r.sismember('comp', payload['name']) == 1:
		return jsonify({'error': 'CONFLICT'}), 409

	if r.sismember('emails', payload['email']):
		return jsonify({'error': 'Email already in use'}), 409

	id = get_new_id('comp_id')
	r.sadd('comp', payload['name'])
	r.sadd('comp_ids', id)
	r.sadd('emails', payload['email'])
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
	if 'comp_type' in data:
		r.hset(company_id, 'comp_type', data['comp_type'])

	return jsonify({'message': r.hgetall(company_id)})


# Delete company by ID
@app.route('/io/company/<string:company_id>', methods=['DELETE'])
def delete_company(company_id):
	if r.sismember('comp_ids', company_id):
		r.srem('comp', r.hget(company_id, 'name'))
		r.srem('comp_ids', company_id)
		r.srem('emails', r.hget(company_id, 'email'))
		r.delete(company_id)

		emps = r.smembers('emp_ids')

		for emp in emps:
			if r.hgetall(emp)['id_comp'] == company_id:
				r.srem('emp_ids', emp)
				r.srem('emails', r.hget(emp, 'email'))
				r.delete(emp)


		return jsonify({'message': 'Company deleted successfully'})

	return jsonify({'message': 'SUCCESS'}), 200


@app.route('/io/employee', methods=['POST'])
def create_employee():
	payload = request.get_json()

	company_data = r.hgetall(payload['id_comp'])
	if not company_data:
		return jsonify({'error': 'Company not found'}), 404

	if r.sismember('emails', payload['email']):
		return jsonify({'error': 'Email already in use'}), 409

	id = get_new_id('emp_id')
	r.sadd('emp_ids', id)
	r.sadd('emails', payload['email'])
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

	if 'first_name' in data:
		r.hset(id, 'first_name', data['first_name'])
	if 'last_name' in data:
		r.hset(id, 'last_name', data['last_name'])
	if 'address' in data:
		r.hset(id, 'address', data['address'])
	if 'phone_number' in data:
		r.hset(id, 'phone_number', data['phone_number'])

	return jsonify({'message': r.hgetall(id)})


@app.route('/io/employee/<string:id>', methods=['DELETE'])
def delete_employee(id):
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	if r.sismember('emp_ids', id):
		r.srem('emp_ids', id)
		r.srem('emails', r.hget(id, 'email'))
		r.delete(id)
		return jsonify({'message': 'Company deleted successfully'})

	return jsonify({'message': 'SUCCESS'}), 200


@app.route('/io/employee/<string:id>/books/active', methods=['POST'])
def add_active_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	if not r.sismember('book_ids', book_id):
		return jsonify({'error': 'Book not found'}), 404

	r.sadd(f'{id}:books:active', book_id)
	return jsonify({'message': 'Book added to active list successfully'})


@app.route('/io/employee/<string:id>/books/wishlist', methods=['POST'])
def add_wishlist_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	if not r.sismember('book_ids', book_id):
		return jsonify({'error': 'Book not found'}), 404

	r.sadd(f'{id}:books:wishlist', book_id)
	return jsonify({'message': 'Book added to wishlist successfully'})


@app.route('/io/employee/<string:id>/books/listened', methods=['POST'])
def add_listened_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	if not r.sismember('book_ids', book_id):
		return jsonify({'error': 'Book not found'}), 404

	r.sadd(f'{id}:books:listened', book_id)
	return jsonify({'message': 'Book added to listened list successfully'})


@app.route('/io/employee/<string:id>/books', methods=['GET'])
def get_employee_books(id):
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	active_books = list(r.smembers(f'{id}:books:active'))
	wishlist_books = list(r.smembers(f'{id}:books:wishlist'))
	listened_books = list(r.smembers(f'{id}:books:listened'))

	books = []

	# Check if books still exists
	for book in active_books:
		if r.sismember('book_ids', book):
			books.append(book)

	active_books = books[:]
	books = []

	for book in wishlist_books:
		if r.sismember('book_ids', book):
			books.append(book)

	wishlist_books = books[:]
	books = []

	for book in listened_books:
		if r.sismember('book_ids', book):
			books.append(book)

	listened_books = books[:]

	return jsonify({
		'active_books': active_books,
		'wishlist_books': wishlist_books,
		'listened_books': listened_books
	})


@app.route('/io/employee/<string:id>/books/active', methods=['DELETE'])
def delete_active_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	r.srem(f'{id}:books:active', book_id)
	return jsonify({'message': 'Book removed from active list successfully'})


@app.route('/io/employee/<string:id>/books/wishlist', methods=['DELETE'])
def delete_wishlist_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	r.srem(f'{id}:books:wishlist', book_id)
	return jsonify({'message': 'Book removed from wishlist successfully'})


@app.route('/io/employee/<string:id>/books/listened', methods=['DELETE'])
def delete_listened_book(id):
	book_id = request.json.get('book_id')
	if not r.exists(id):
		return jsonify({'error': 'Employee not found'}), 404

	r.srem(f'{id}:books:listened', book_id)
	return jsonify({'message': 'Book removed from listened list successfully'})


@app.route("/io/book", methods=["POST"])
def post_book():
	payload = request.get_json()

	# If the book already exists, then we return 409
	if r.sismember('book', payload['name']) == 1:
		return jsonify({'error': 'CONFLICT'}), 409

	id = get_new_id('book_id')
	r.sadd('book', payload['name'])
	r.sadd('book_ids', id)
	r.hset(id, mapping={
		'name': payload['name'],
		'author': payload['author'],
		'length': payload['length'],
		'publish_date': payload['publish_date'],
		'description': payload['description'],
		'book_type': payload['book_type'],
		'link': payload['link']
	})

	return jsonify({'id': id}), 201


@app.route('/io/book/<string:book_id>', methods=['GET'])
def get_book(book_id):
	book_data = r.hgetall(book_id)
	if not book_data:
		return jsonify({'error': 'Book not found'}), 404
	return jsonify({
		'id': book_id,
		'name': book_data['name'],
		'author': book_data['author'],
		'length': book_data['length'],
		'publish_date': book_data['publish_date'],
		'description': book_data['description'],
		'book_type': book_data['book_type'],
		'link': book_data['link']
	})


@app.route('/io/book', methods=['GET'])
def get_all_books():
	books = []
	keys = r.smembers('book_ids')
	for key in keys:
		book_data = r.hgetall(key)
		books.append({
			'id': key,
			'name': book_data['name'],
			'author': book_data['author'],
			'length': book_data['length'],
			'publish_date': book_data['publish_date'],
			'description': book_data['description'],
			'book_type': book_data['book_type'],
			'link': book_data['link']
		})
	return jsonify(books)


@app.route('/io/book/<string:book_id>', methods=['PUT'])
def update_book(book_id):
	data = request.json
	book_data = r.hgetall(book_id)
	if not book_data:
		return jsonify({'error': 'Book not found'}), 404

	if 'name' in data:
		r.hset(book_id, 'name', data['name'])
	if 'author' in data:
		r.hset(book_id, 'author', data['author'])
	if 'length' in data:
		r.hset(book_id, 'length', data['length'])
	if 'publish_data' in data:
		r.hset(book_id, 'publish_date', data['publish_date'])
	if 'description' in data:
		r.hset(book_id, 'description', data['description'])
	if 'book_type' in data:
		r.hset(book_id, 'book_type', data['book_type'])
	if 'link' in data:
		r.hset(book_id, 'link', data['link'])

	return jsonify({'message': r.hgetall(book_id)})


@app.route('/io/book/<string:book_id>', methods=['DELETE'])
def delete_book(book_id):
	if r.sismember('book_ids', book_id):
		r.srem('book', r.hget(book_id, 'name'))
		r.srem('book_ids', book_id)
		r.delete(book_id)
		return jsonify({'message': 'Book deleted successfully'})

	return jsonify({'message': 'SUCCESS'}), 200


if __name__ == '__main__':
   app.run('0.0.0.0', debug=True)
