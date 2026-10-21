import os
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg
from psycopg.rows import dict_row

from auth import ApiError, decode_token, password_hash, password_matches, tokens


@contextmanager
def database(write=False):
    with psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row) as db:
        if write:
            # Для небольшого учебного сервиса изменения идут по очереди.
            # Блокировка снимается автоматически при commit/rollback.
            db.execute('SELECT pg_advisory_xact_lock(1)')
        yield db


def require_role(user, roles):
    if user['role'] not in roles:
        raise ApiError('ACCESS_DENIED', 403, 'Недостаточно прав')


def validation(field, message):
    raise ApiError('VALIDATION_ERROR', 400, 'Некорректные данные', {field: message})


def money(value):
    return Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def create_admin():
    with database() as db:
        db.execute("INSERT INTO users(username,password_hash,role) VALUES ('admin',%s,'ADMIN') ON CONFLICT DO NOTHING",
                   (password_hash(os.environ['ADMIN_PASSWORD']),))


def register(body):
    with database() as db:
        row = db.execute('INSERT INTO users(username,password_hash,role) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id,username,role',
                         (body['username'], password_hash(body['password']), body['role'])).fetchone()
        if not row:
            raise ApiError('USER_EXISTS', 409, 'Имя пользователя занято')
        return row


def login(body):
    with database() as db:
        user = db.execute('SELECT * FROM users WHERE username=%s', (body['username'],)).fetchone()
    if not user or not password_matches(body['password'], user['password_hash']):
        raise ApiError('INVALID_CREDENTIALS', 401, 'Неверный логин или пароль')
    return tokens(user)


def refresh(body):
    user = decode_token(body['refresh_token'], 'refresh')
    with database() as db:
        user = db.execute('SELECT id,role FROM users WHERE id=%s', (user['id'],)).fetchone()
    if not user:
        raise ApiError('REFRESH_TOKEN_INVALID', 401, 'Пользователь не найден')
    return tokens(user)


def product(db, id):
    row = db.execute('SELECT * FROM products WHERE id=%s', (id,)).fetchone()
    if not row:
        raise ApiError('PRODUCT_NOT_FOUND', 404, 'Товар не найден')
    return row


def check_seller(user, row):
    if user['role'] != 'ADMIN' and row['seller_id'] != user['id']:
        raise ApiError('ACCESS_DENIED', 403, 'Товар принадлежит другому продавцу')


def create_product(body, user, seller_id=None):
    owner = seller_id if seller_id is not None else user['id']
    if user['role'] != 'ADMIN' and owner != user['id']:
        raise ApiError('ACCESS_DENIED', 403, 'Нельзя создать чужой товар')
    with database(True) as db:
        seller = db.execute("SELECT id FROM users WHERE id=%s AND role IN ('SELLER','ADMIN')", (owner,)).fetchone()
        if not seller:
            validation('seller_id', 'Продавец не найден')
        return db.execute('INSERT INTO products(name,description,price,stock,category,status,seller_id) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *',
                          (body['name'], body['description'], body['price'], body['stock'], body['category'], body['status'], owner)).fetchone()


def get_product(id, user):
    with database() as db:
        return product(db, id)


def list_products(user, page=0, size=20, status=None, category=None):
    where, params = [], []
    for column, value in [('status', status), ('category', category)]:
        if value is not None:
            where.append(column + '=%s'); params.append(value)
    clause = ' WHERE ' + ' AND '.join(where) if where else ''
    with database() as db:
        total = db.execute('SELECT count(*) AS n FROM products' + clause, params).fetchone()['n']
        rows = db.execute('SELECT * FROM products' + clause + ' ORDER BY id LIMIT %s OFFSET %s', params + [size, page * size]).fetchall()
        return {'items': rows, 'totalElements': total, 'page': page, 'size': size}


def update_product(id, body, user):
    with database(True) as db:
        check_seller(user, product(db, id))
        return db.execute('UPDATE products SET name=%s,description=%s,price=%s,stock=%s,category=%s,status=%s WHERE id=%s RETURNING *',
                          (body['name'], body['description'], body['price'], body['stock'], body['category'], body['status'], id)).fetchone()


def delete_product(id, user):
    with database(True) as db:
        check_seller(user, product(db, id))
        db.execute("UPDATE products SET status='ARCHIVED' WHERE id=%s", (id,))


def order(db, id, user):
    row = db.execute('SELECT * FROM orders WHERE id=%s', (id,)).fetchone()
    if not row:
        raise ApiError('ORDER_NOT_FOUND', 404, 'Заказ не найден')
    if user['role'] != 'ADMIN' and row['user_id'] != user['id']:
        raise ApiError('ORDER_OWNERSHIP_VIOLATION', 403, 'Чужой заказ')
    return row


def order_response(db, id, user):
    result = order(db, id, user)
    result['items'] = db.execute('SELECT id,product_id,quantity,price_at_order FROM order_items WHERE order_id=%s ORDER BY id', (id,)).fetchall()
    return result


def get_order(id, user):
    with database() as db:
        return order_response(db, id, user)


def rate_limit(db, user_id, operation):
    last = db.execute('SELECT created_at FROM user_operations WHERE user_id=%s AND operation_type=%s ORDER BY created_at DESC LIMIT 1', (user_id, operation)).fetchone()
    interval = timedelta(minutes=float(os.environ.get('ORDER_INTERVAL_MINUTES', '1')))
    if last and datetime.now(timezone.utc) - last['created_at'] < interval:
        raise ApiError('ORDER_LIMIT_EXCEEDED', 429, 'Операции выполняются слишком часто')


def record_operation(db, user_id, operation):
    db.execute('INSERT INTO user_operations(user_id,operation_type) VALUES (%s,%s)', (user_id, operation))


def reserve_items(db, items):
    quantities = Counter()
    for item in items:
        quantities[item['product_id']] += item['quantity']
    products = {}
    for id in quantities:
        row = product(db, id)
        if row['status'] != 'ACTIVE':
            raise ApiError('PRODUCT_INACTIVE', 409, 'Товар неактивен')
        products[id] = row
    missing = [{'product_id': id, 'requested': quantity, 'available': products[id]['stock']}
               for id, quantity in quantities.items() if products[id]['stock'] < quantity]
    if missing:
        raise ApiError('INSUFFICIENT_STOCK', 409, 'Недостаточно товаров', {'items': missing})
    for id, quantity in quantities.items():
        db.execute('UPDATE products SET stock=stock-%s WHERE id=%s', (quantity, id))
    # Сохраняем каждую позицию отдельно: лимит 999 относится к позиции.
    return [(products[item['product_id']], item['quantity']) for item in items]


def save_items(db, order_id, rows):
    for row, quantity in rows:
        db.execute('INSERT INTO order_items(order_id,product_id,quantity,price_at_order) VALUES (%s,%s,%s,%s)', (order_id, row['id'], quantity, row['price']))


def return_stock(db, order_id):
    db.execute('UPDATE products p SET stock=p.stock+i.quantity FROM (SELECT product_id, SUM(quantity) AS quantity FROM order_items WHERE order_id=%s GROUP BY product_id) i WHERE p.id=i.product_id', (order_id,))


def promo_discount(db, promo, total, already_used=False):
    now = datetime.now(timezone.utc)
    if not promo or not promo['active'] or not promo['valid_from'] <= now <= promo['valid_until'] or promo['current_uses'] - int(already_used) >= promo['max_uses']:
        raise ApiError('PROMO_CODE_INVALID', 422, 'Промокод недоступен')
    if total < promo['min_order_amount']:
        raise ApiError('PROMO_CODE_MIN_AMOUNT', 422, 'Недостаточная сумма заказа')
    if promo['discount_type'] == 'PERCENTAGE':
        # В условии: свыше 70% скидка не применяется, цена остаётся 100%.
        discount = total * promo['discount_value'] / 100 if promo['discount_value'] <= 70 else Decimal(0)
    else:
        discount = min(promo['discount_value'], total)
    return money(discount)


def create_order(body, user):
    with database(True) as db:
        rate_limit(db, user['id'], 'CREATE_ORDER')
        active = db.execute("SELECT id FROM orders WHERE user_id=%s AND status IN ('CREATED','PAYMENT_PENDING')", (user['id'],)).fetchone()
        if active:
            raise ApiError('ORDER_HAS_ACTIVE', 409, 'Уже есть активный заказ')
        rows = reserve_items(db, body['items'])
        total = sum((p['price'] * q for p, q in rows), Decimal(0))
        promo_id, discount = None, Decimal(0)
        if body.get('promo_code'):
            promo = db.execute('SELECT * FROM promo_codes WHERE code=%s', (body['promo_code'],)).fetchone()
            discount = promo_discount(db, promo, total)
            promo_id = promo['id']
            db.execute('UPDATE promo_codes SET current_uses=current_uses+1 WHERE id=%s', (promo_id,))
        id = db.execute("INSERT INTO orders(user_id,status,promo_code_id,total_amount,discount_amount) VALUES (%s,'CREATED',%s,%s,%s) RETURNING id", (user['id'], promo_id, total-discount, discount)).fetchone()['id']
        save_items(db, id, rows)
        record_operation(db, user['id'], 'CREATE_ORDER')
        return order_response(db, id, user)


def update_order(id, body, user):
    with database(True) as db:
        old = order(db, id, user)
        if old['status'] != 'CREATED':
            raise ApiError('INVALID_STATE_TRANSITION', 409, 'Менять позиции можно только в CREATED')
        rate_limit(db, user['id'], 'UPDATE_ORDER')
        return_stock(db, id)
        rows = reserve_items(db, body['items'])
        total = sum((p['price'] * q for p, q in rows), Decimal(0))
        promo_id, discount = old['promo_code_id'], Decimal(0)
        if promo_id:
            promo = db.execute('SELECT * FROM promo_codes WHERE id=%s', (promo_id,)).fetchone()
            if total < promo['min_order_amount']:
                db.execute('UPDATE promo_codes SET current_uses=current_uses-1 WHERE id=%s', (promo_id,))
                promo_id = None
            else:
                discount = promo_discount(db, promo, total, already_used=True)
        db.execute('DELETE FROM order_items WHERE order_id=%s', (id,))
        save_items(db, id, rows)
        db.execute('UPDATE orders SET promo_code_id=%s,total_amount=%s,discount_amount=%s WHERE id=%s', (promo_id, total-discount, discount, id))
        record_operation(db, user['id'], 'UPDATE_ORDER')
        return order_response(db, id, user)


def cancel_order(id, user):
    with database(True) as db:
        old = order(db, id, user)
        if old['status'] not in ('CREATED','PAYMENT_PENDING'):
            raise ApiError('INVALID_STATE_TRANSITION', 409, 'Этот заказ нельзя отменить')
        return_stock(db, id)
        if old['promo_code_id']:
            db.execute('UPDATE promo_codes SET current_uses=current_uses-1 WHERE id=%s', (old['promo_code_id'],))
        db.execute("UPDATE orders SET status='CANCELED' WHERE id=%s", (id,))
        return order_response(db, id, user)


def change_order_status(id, body, user):
    transitions = {'CREATED':'PAYMENT_PENDING','PAYMENT_PENDING':'PAID','PAID':'SHIPPED','SHIPPED':'COMPLETED'}
    with database(True) as db:
        old = order(db, id, user)
        if transitions.get(old['status']) != body['status']:
            raise ApiError('INVALID_STATE_TRANSITION', 409, 'Недопустимый переход')
        db.execute('UPDATE orders SET status=%s WHERE id=%s', (body['status'], id))
        return order_response(db, id, user)


def create_promo(body, user):
    if body['valid_from'].tzinfo is None or body['valid_until'].tzinfo is None:
        validation('valid_from/valid_until', 'Укажите часовой пояс, например Z')
    if body['valid_until'] < body['valid_from']:
        validation('valid_until', 'Окончание раньше начала')
    with database(True) as db:
        row = db.execute('INSERT INTO promo_codes(code,discount_type,discount_value,min_order_amount,max_uses,valid_from,valid_until,active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING *',
                         tuple(body[k] for k in ('code','discount_type','discount_value','min_order_amount','max_uses','valid_from','valid_until','active'))).fetchone()
        if not row:
            raise ApiError('PROMO_CODE_EXISTS', 409, 'Код уже существует')
        return row
