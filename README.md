# ETL_Project 
## Georgia Tech ETL Project:

- Doris Akpovwa
- Isaih Peek
- Carlos Pazos


## you will scrape the following information from http://quotes.toscrape.com/

- quote text
- tags
- Author name
- Author Details
  - born
  - description

## Store the collected information into MongoDB( either local mongoDB or Atlas mongoDB)

## Design the following three tables, Extract the data from mongoDB and load it into postgres

- one table for quotes, this table can have the quote and author relationship (id, author_name, text )
- one table for author information (name, born , description)
- one table to store quote and tag relation (quote_id , tag)

## Create a FLASK application with the following endpoints

<details>
<summary><strong>/home or / </strong></summary>
	
	This route should display the available routes :

</details>	
	
<details>
    <summary><strong>/quotes </strong></summary>

This route will dispaly all the available quotes in the database.

```
{
    total: <total number quotes scraped >,
    quotes : [
                {
                    text: <quote text >,
                    author name: <author name >,
                    tags: []
                },
	            ...
	        ]
}

```

</details>

<details>
    <summary><strong>/authors </strong></summary>

This route will display the information about all the authors available in the database.

```
{
    total: <total number of authors>,
    details:[
            	{
            		name : <author name >,
            		description : <author description>,
            		born : <date of birth etc. >,
            		count : <total number of quotes by this author >,
            		quotes : [
                				{
                    				text: <quote text>,
                    				tags: []
                				},
            		...
            		]
            	},
        	...
    	]
}
```

</details>

<details>
    <summary><strong>/authors/< author name > </strong></summary>

This route will display the information about a particular author.

```
{
    name: <Author name>,
    description: <author description>,
    born: <date of birth etc>
    number_of_quotes :  <total quotes by the author>
    quotes : [
    		{
    			text: <quote text>,
    			tags: []
    		},
            ...
    	]
}
```

</details>

<details>
    <summary><strong>/tags </strong></summary>

this route will dispaly all the available tags in the database.

```
{
	count: <total tags>,
	details:[
        		{
        			name: < tag>,
        			number_of_quotes :  <total quotes this tag appears in >
        			quotes : [
                				{
                					text: <quote text>,
                					tags: []
                				},
                                ...
    				        ]
        		},
            ...
	]
}

```

</details>

<details>	
    <summary><strong>/tags/< tag > </strong></summary>

this route will display the information about a particual tag only.

```
{
	tag : <tag name>,
	count : <number of quotes this tag appears in >,
	quotes : [
			{
    			quote : <quote text >,
    			tags : []
			},
		...
		]

}

```

</details>

<details>
   <summary><strong>/top10tags </strong></summary>

This route will display the information about top10 tags.

```
	[
		{
		tag: < tag name > ,
		quote count: < number of quotes this tag appears in >
		},
		...
	]

```

</details>
