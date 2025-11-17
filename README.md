# LeaderboardBD
A repository dedicated to cataloguing every account ever created on [WWW KoGaMa](https://www.kogama.com), designed to systematically identify and trace bot activity, boosting behaviour, harassment patterns, and other forms of suspicious or inappropriate conduct.
Flagged accounts are recorded here to support further investigation, reporting, and potential removal.   
- - - 

## API

The enpoint being used is ``api/leaderboard/top/``.  We use pagination due to the ``count`` limit of ``400`` per each page. This way we can keep fetching data in small batches and storing in a safe file to later sort through and create flags and libraries of flagged accounts.  
Build URL: ``{ENDPOINT}?count={COUNT}&page={page}``

## Catalogue

[***__LeaderboardDB/Hits/Bots__***](/Hits/Bots) - A storage of all accounts that have been flagged with bot behaviour, usually boosted to levels between 8 to 22. Used to buy avatars & models from main accounts of the abusers, or to advertise illegitimate files and services.
