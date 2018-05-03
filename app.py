from flask import Flask, render_template, flash, redirect, url_for, session, request, logging, jsonify
from flask_mysqldb import MySQL
from wtforms import Form, StringField, TextAreaField, PasswordField, validators
import csv, ast, os

UPLOAD_FOLDER = 'raw_data/uploads'
ALLOWED_EXTENSIONS = set(['csv'])

app = Flask(__name__)

# Config folder for data upload
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# Config maximum upload to 8 MB
app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024
# Config MySQL
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'myflaskapp'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

#init MYSQL
mysql = MySQL(app)

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/graph', methods =['GET','POST'])
def graph(chartID = 'chart_ID', chart_type = 'line', chart_height = 500):

    # Initial Values. Will change based on selections
    table1 = 'dataset_asm_product_311119b'
    table2 = 'dataset_asm_product_311119m'

    try:
        if request.method == "POST":
            selection = request.form.get('graphselect')
            if table1 != selection:
                table2 = selection
            flash(selection)

        cursor = mysql.connection.cursor()
        cursor.execute('select table_name from information_schema.tables where table_schema = "myflaskapp";')
        table_names = [x['table_name'] for x in cursor.fetchall()]
        table = table_names[3];
        statement = 'select * from ' + table + ';'
        cursor.execute(statement)
        headers = [i[0] for i in cursor.description]
        results = cursor.fetchall()
        cursor.close()
        # Keep only 20 records
        if len(results) > 20:
            results = results[0:20]

    except Exception as e:
        flash(e)
        return render_template("graph.html")

    statement = 'select * from ' + table1 + ' natural join ' + table2 + ' ;'
    # Create cursor
    cur = mysql.connection.cursor()
    cur.execute(statement)
    headers = [i[0] for i in cur.description]
    col1 = headers[1]
    col2 = headers[2]
    results = cur.fetchall()
    years = [x['YEAR'] for x in results]
    data_series1 = [x[col1] for x in results]
    data_series2 = [x[col2] for x in results]
    cur.close()
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    series = [{"name": col1 ,"data": data_series1}, {"name": col2 ,"data": data_series2}]
    title = {"text": table1}
    xAxis = {"categories": years}
    yAxis = {"title": {"text": table1}, "format": '{value:.2f}'}
    return render_template('graph.html', table_names=table_names, chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/import')
def importData():
    return render_template('import.html')

@app.route('/upload', methods = ['GET', 'POST'])
def upload():
    try:
        if request.method == "POST":
            # Get values from the form
            attempted_datatable_name = request.form['tablename']
            attempted_datavalue = request.form['datavalue']
            attempted_description = request.form['description']
            attempted_source = request.form['source']

            # Validation for field values
            if attempted_datatable_name == '':
                flash('Table name cannot be empty')
                return redirect(request.url)
            if attempted_datavalue == '':
                flash('Value cannot be empty')
                return redirect(request.url)
            if attempted_description == '':
                flash('Description cannot be empty')
                return redirect(request.url)
            if attempted_source == '':
                flash('Source cannot be empty')
                return redirect(request.url)

            # check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
            f = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if f.filename == '':
                flash('No selected file')
                return redirect(request.url)
            if f:
                filename = f.filename
                f.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                f_name, f_extension = os.path.splitext(filename)
                csv_file = app.config['UPLOAD_FOLDER'] + '/' + filename
                with open(csv_file, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    headers = next(reader)
                    # csv needs to be in form {YEAR, whatever}. Check that
                    if len(headers) != 2:
                        flash('File has more than two Fields')
                        return redirect(request.url)
                    if headers[0] != 'YEAR':
                        flash('File needs to be exactly two fields [YEAR, value]')
                        return redirect(request.url)
                    # If the csv is in the correct format, compute the min/max of years
                    years = []
                    for row in reader:
                        years.append(row[0])
                # Done with reading the csv, now if everything is good, insert to that meta data table
                min_year = min(years)
                max_year = max(years)
                attempted_datatable_name = attempted_datatable_name.replace(" ", "_")
                attempted_datavalue = attempted_datavalue.replace(" ", "_")

                cur = mysql.connection.cursor()
                cur.execute('insert into data_sources_meta_data (tbl_name, val, min_year, max_year, val_desc, source) values (%s,%s,%s,%s,%s,%s)',(attempted_datatable_name, attempted_datavalue, min_year, max_year, attempted_description, attempted_source))
                mysql.connection.commit()
                cur.close()

                populate(csv_file, attempted_datatable_name)

                flash('Successfully Imported Database')
                return redirect('import')

            return redirect(url_for('importData'))

        return render_template("import.html")

    except Exception as e:
        flash(e)
        return render_template("import.html")

def populate(csvfile, tname):
    tablename = tname
    f = open(csvfile, 'r')
    reader = csv.reader(f)

    # Arrays to store columns and their datatypes
    data, headers, type_list = [], [], []
    clean_data, clean_headers, clean_type = [], [], []

    # We can iterate over the rows in our CSV, call the dataType function to check its datatype and populate
    for row in reader:
        if len(headers) == 0:
            headers = [k.replace(" ", "_") for k in row]
            for col in row:
                type_list.append('')
        else:
            data.append(row)
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type

    f.close()

    safe_col = []
    # Clean the data by only saving the columns that are not strings
    for i in range(len(headers)):
        if type_list[i] != 'varchar':
            safe_col.append(i)

    for col in safe_col:
        clean_headers.append(headers[col].lower())
        clean_type.append(type_list[col].lower())

    clean_data = [[each_list[i] for i in safe_col] for each_list in data]

    # Create cursor
    cur = mysql.connection.cursor()

    ### Region: Create Table based on clean csv header ###
    create_statement = 'create table ' + tablename +' ('

    for i in range(len(clean_headers)):
        create_statement = (create_statement + '{} {}' + ',').format(clean_headers[i], clean_type[i])

    create_statement = create_statement[:-1] + ');'

    # Execute and commit to DB
    cur.execute(create_statement)
    mysql.connection.commit()
    ### EndRegion ###

    ### Region: Insert into table clean (non string) data  ###
    for row in clean_data:
        insert_statement = 'insert into ' + tablename +' ('

        for header in clean_headers:
            insert_statement = (insert_statement + '{}' + ',').format(header)

        insert_statement = insert_statement[:-1] + ') values ('

        for col in row:
            insert_statement = (insert_statement + '{}' + ',').format(col)

        insert_statement = insert_statement[:-1] + ');'

            # Execute and commit to DB
        cur.execute(insert_statement)
        mysql.connection.commit()
    ### EndRegion ###

    # Close Connection
    cur.close()
    return

#  find the data type for each row
def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'

    if type(t) in [int, float]:
        if type(t) is int and current_type not in ['float', 'varchar']:
            # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'float(5,2)'
        else:
            return 'varchar'

@app.route('/tables', methods =['GET','POST'])
def tables():

    try:
        table = ""
        if request.method == "POST":
            selection = request.form.get('tableselect')
            table = selection

        cursor = mysql.connection.cursor()
        cursor.execute('select table_name from information_schema.tables where table_schema = "myflaskapp";')
        table_names = [x['table_name'] for x in cursor.fetchall()]
        if table == "":
            table = table_names[3];
        statement = 'select * from ' + table + ';'
        cursor.execute(statement)
        headers = [i[0] for i in cursor.description]
        results = cursor.fetchall()
        cursor.close()
        # Keep only 20 records
        if len(results) > 20:
            results = results[0:20]
        return render_template('tables.html', table_names=table_names, headers=headers, results=results)

    except Exception as e:
        flash(e)
        return render_template("tables.html")



if __name__ == '__main__':
    app.secret_key='secretkey123'
    app.run(debug=True)
