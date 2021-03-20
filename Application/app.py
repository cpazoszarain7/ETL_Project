from flask import Flask, render_template, redirect
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import pandas as pd

################################################
# Database Setup
#################################################
#Connect to Postgress
rds_connection_string = "carlospazos@127.0.0.1:5432/quotes_db"
engine = create_engine(f'postgresql://{rds_connection_string}')

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
author = Base.classes.author
quotes = Base.classes.quotes
tag = Base.classes.tag

# Create an instance of Flask
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Route to render index.html template using data from Mongo
@app.route("/")
def home():



    # Return template and data
    return render_template("index.html")

@app.route('/quotes')
def quotes_func():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Get all precipitation values by date
    text = session.query(quotes.text).all()
    name = session.query(quotes.author_name).all()
    tags = session.query(tag.tag).all()

    #Close session
    session.close()

    

    # Convert list of results into a dictionary
    quote_array = []
    for x in range(0,len(text)):
        
        quote_array.append(

            {
                'text': text[x][0],
                'author name':name[x][0],
                'tags':tags[x][0]


            }


        )

    #Build Output Dictionary
    output_dic = {
        'total': str(len(text)) + " Quotes found",
        'quotes':quote_array
        
    }
    
    return jsonify(output_dic)
        
@app.route('/authors')
def authors_func():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Get list of unique authors
    list_authors = session.query(func.distinct(author.author_name)).all()
    list_authors = [r for (r,) in list_authors]

    #Iterate through the list of unique authors

    #Build list with dictionaries for output
    details = []
    for x in range(0,len(list_authors)):
        auth_dic={}
        auth_dic['name']= list_authors[x],
        auth_dic['description']= session.query(func.distinct(author.description)).filter(author.author_name==list_authors[x]).one()[0]
        auth_dic['born']= session.query(func.distinct(author.born)).filter(author.author_name==list_authors[x]).one()[0]
        auth_dic['count'] = session.query(func.count(author.author_name)).filter(author.author_name==list_authors[x]).one()[0]

        quotesli =[]
        total_quotes_author = session.query(quotes.text,tag.tag).filter(quotes.id==tag.quote_id).filter(quotes.id==author.author_id).filter(author.author_name==list_authors[x]).all()
        author_count = session.query(func.count(author.author_name)).filter(author.author_name==list_authors[x]).one()[0]
        for y in range(0,author_count):
            quote_dic={}
            quote_dic['text']= total_quotes_author[y][0]
            quote_dic['tags']= total_quotes_author[y][1]
            quotesli.append(quote_dic)
        
        auth_dic['quotes']=quotesli

        details.append(auth_dic)        
    

    #Close session
    session.close()

    return jsonify(details)

@app.route('/authors/<authorname>')
def authorsbyname_func(authorname):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Search author based on partial string and not case sensitive
    sauthor = session.query(author.author_name).filter(author.author_name.ilike('%' + authorname + '%')).all()[0]

    #Build list with dictionaries for output
    details = []
    auth_dic={}
    auth_dic['name']= sauthor[0],
    auth_dic['description']= session.query(func.distinct(author.description)).filter(author.author_name==sauthor).one()[0]
    auth_dic['born']= session.query(func.distinct(author.born)).filter(author.author_name==sauthor).one()[0]
    auth_dic['count'] = session.query(func.count(author.author_name)).filter(author.author_name==sauthor).one()[0]

    quotesli =[]
    total_quotes_author = session.query(quotes.text,tag.tag).filter(quotes.id==tag.quote_id).filter(quotes.id==author.author_id).filter(author.author_name==sauthor).all()
    author_count = session.query(func.count(author.author_name)).filter(author.author_name==sauthor).one()[0]
    for y in range(0,author_count):
        quote_dic={}
        quote_dic['text']= total_quotes_author[y][0]
        quote_dic['tags']= total_quotes_author[y][1]
        quotesli.append(quote_dic)
    
    auth_dic['quotes']=quotesli

    details.append(auth_dic)        
    

    #Close session
    session.close()

    return jsonify(details)


@app.route('/tags')
def tags_func():
    # Create our session (link) from Python to the DB
    # session = Session(engine)

    #Create list of unique tags
    all_tags = engine.execute('select tag from tag')
    all_tags_values= all_tags.fetchall()

    tag_list = []

    for x in range(0,len(all_tags_values)):   
        for y in range(0,len(all_tags_values[x][0])):
            tag_list.append(all_tags_values[x][0][y])

    tag_list = list(dict.fromkeys(tag_list))

    #Build list with dictionaries for output
    details = []
    for x in range(0,len(tag_list)):
        tag_dic={}
        tag_dic['name'] = tag_list[x]
        tag_count = engine.execute(f"select count(tag) from tag where tag @> '{{{tag_list[x]}}}'").fetchall()[0]
        tag_dic['number_of_quotes']= tag_count[0]
        
        
        quote_info = engine.execute(f"select tag.tag, quotes.text from tag join quotes on tag.quote_id=quotes.id where tag.tag @> '{{{tag_list[x]}}}'").fetchall()
        quote_data=[]
        for y in range(0,len(quote_info)):
            quote_dic={}
            quote_dic['text']=quote_info[y][1]
            quote_dic['tags']=quote_info[y][0]
            quote_data.append(quote_dic)
        tag_dic['details']=quote_data
        details.append(tag_dic)
    


    return jsonify(details)


@app.route('/tags/<tagname>')
def tagbyname_func(tagname):
    #Build list with dictionaries for tag in variable
    details = []
    tag_dic={}
    tag_dic['name'] = tagname
    tag_count = engine.execute(f"select count(tag) from tag where tag @> '{{{tagname}}}'").fetchall()[0]
    tag_dic['number_of_quotes']= tag_count[0]    
    
    quote_info = engine.execute(f"select tag.tag, quotes.text from tag join quotes on tag.quote_id=quotes.id where tag.tag @> '{{{tagname}}}'").fetchall()
    quote_data=[]
    for y in range(0,len(quote_info)):
        quote_dic={}
        quote_dic['text']=quote_info[y][1]
        quote_dic['tags']=quote_info[y][0]
        quote_data.append(quote_dic)
    tag_dic['details']=quote_data
    details.append(tag_dic)
    


    return jsonify(details)

@app.route('/top10tags')
def top10tags_func():
    #Get list of all tags
    all_tags = engine.execute('select tag from tag')
    all_tags_values= all_tags.fetchall()

    tag_list = []

    for x in range(0,len(all_tags_values)):   
        for y in range(0,len(all_tags_values[x][0])):
            tag_list.append(all_tags_values[x][0][y])

    #Get top 10 tags
    top_10= pd.DataFrame(pd.DataFrame(tag_list)[0].value_counts().head(10))
    top_10.reset_index(inplace=True)
    top_10= top_10['index']

    #Create output dictionaries for each tag in Top 10
    details=[]
    for x in range(0,len(top_10)):
        tag_dic={}
        tag_dic['name'] = top_10[x]
        tag_count = engine.execute(f"select count(tag) from tag where tag @> '{{{top_10[x]}}}'").fetchall()[0]
        tag_dic['tag count'] = tag_count[0]
        details.append(tag_dic)

    return jsonify(details)


if __name__ == "__main__":
    app.run(debug=True)