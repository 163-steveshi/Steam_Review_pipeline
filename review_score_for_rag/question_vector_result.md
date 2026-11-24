-- below is the adhoc test result need to find tune the search query before twekak embediing

check with query example

SELECT c.content,
       c.voted_positive,
       round((1 - (c.embedding <=> q.embedding))::numeric, 3) AS similarity
FROM gold.eval_queries q
CROSS JOIN LATERAL (
    SELECT content, voted_positive, embedding
    FROM gold.review_chunks
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> q.embedding
    LIMIT 10
) c
WHERE q.question LIKE '%What do players think about unlocking new islands%'

ORDER BY similarity DESC;


With a filter, e.g. complaints from negative reviews only. Put the filter inside the subquery so you still get 5 results:
WHERE embedding IS NOT NULL AND voted_positive = false



=== What do players think about the co-op with friends?
  0.366 👍 Awesome co-op experience. The smoothest, most trouble-free hosting and joining. Finished the main content, ignoring achi
  0.369 👍 this is a great co-op game if you say its friend slop go cry about you not having any friends
  0.372 👍 So much fun to co-op with friends
  0.380 👍 Great concept and a lot of fun with friends! Hats off to the devs, they made something unique in a coop space that is so
  0.381 👍 Highly recommend playing it with friends in a co-op!







=== Is the game fun to play solo?
  0.339 👍 really fun game, even by yourself. if you were thinking of buying it and playing solo, definitely do it!
  0.353 👍 Honestly, i went into this thinking it would be pretty boring to play alone. But honestly, i'm like 4 hours in rn and it
  0.365 👍 Its fun even when you play solo
  0.371 👍 Unique & lots of fun. I play completely solo.
  0.379 👍 okay. yeah. extremely goddamn fun. alone it's a bit tedious, but what do you expect? everything is tedious alone.
even w

=== What are the most common complaints?
SELECT c.content,
       c.voted_positive,
       round((1 - (c.embedding <=> q.embedding))::numeric, 3) AS similarity
FROM gold.eval_queries q
CROSS JOIN LATERAL (
    SELECT content, voted_positive, embedding
    FROM gold.review_chunks
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> q.embedding
    LIMIT 10
) c
WHERE q.question LIKE '%What are the most common complaints%'
and c.voted_positive = 'false'

ORDER BY similarity DESC;

=== Are there bugs, crashes or performance problems?

SELECT c.content,
       c.voted_positive,
       round((1 - (c.embedding <=> q.embedding))::numeric, 3) AS similarity
FROM gold.eval_queries q
CROSS JOIN LATERAL (
    SELECT content, voted_positive, embedding
    FROM gold.review_chunks
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> q.embedding
    LIMIT 10
) c
WHERE q.question LIKE '%Are there bugs, crashes or performance problems%'

ORDER BY similarity DESC;

  0.450 👍 Can yall do something about these bugs? I can't go 2 minutes without my game freezing or getting kicked
  0.485 👎 Good game, WAY TOO MANY BUGS AND CRASHES. Can't play properly ever. wait till bugs are all fixed before purchasing. Quit
  0.489 👎 Game is fun but to many bugs and game crashes way to much. I cant enjoy it anymore
  0.517 👍 I crash on 5 min sharp please fix ts bs. Overall nice game.
  0.521 👎 The game is really fun and all with friends and shit but its too buggy and server are lagging hard. I crashed just to jo

=== How do players feel about upgrading gear and buying equipment? (bad vector content need better value eg detail weapon: shotgun and rafile)
  0.577 👍 Weapons, gear — and — general — escalation

What really sold me — was how quickly — the game escalates — from "harmless
  0.590 👍 watched my friend spend 20k on a sniper, when the over had a standard shot gun. 🥀
  0.611 👍 It's nice but...the only thing that is bad that the inventory drops and sometimes might get lost! if you die and respawn
  0.613 👍 please add more islands and weapons and a locker for your guns. i cant say this enough my guns flew or launched to the s
  0.615 👍 What I would like, more islands, rods I mean like a rod with high luck but low drip chance (or the opposite), baits, cus

=== What do players think about unlocking new islands?
  0.397 👍 PLEASE WE NEED MORE ISLANDS!!
  0.401 👍 wish there was more islands

edit: they added more islands
  0.405 👍 The update 1.0.11 just came out, and it's supposed to be six islands in total? In the update says that the gamble table
  0.408 👍 new idea game thats good but can u add some new islands in the future
  0.408 👍 Cant wait for more islands

=== What do players say about the fish gambling mechanic?
  0.323 👎 Speaking of the Roulette table, it is probably one of the worst thing about this game.
So the Roulette table is a way of
  0.332 👍 dammit stop gambling the fish
  0.342 👍 I seriously wonder how a fucking fish can get sniped from point blank with a 50.cal sniper and still proceed to jump in
  0.343 👍 You can gamble on the fish 10/10
  0.344 👍 I'm hitting 1080 trickshots on fish for extra money then gambling it all on red every single time

I have learned nothin

=== Do people enjoy collecting rare fish variants?(answer is poor): better use detail key word, goldfish, tuna, cod
  0.487 👍 cool game i like it. But gambling fish is too rare. I WANT MORE GAMBLE
  0.494 👍 Cool game.
I just feel like there aren't enough things to do though.
Only around 50 different fish with just one mutatio
  0.497 👍 If you want to spend 20 times the playtime searching for a damn goldfish then I think you should absolutely NOT get the
  0.497 👍 amazing game I genuinely experinced a lot with the fish and my favorite part Gambling The bosses where a lot of fun i ju
  0.505 👍 Salmon, Tuna, Cod, Trout, Bass, Catfish, Snapper, Grouper, Halibut, Mackerel, Sardine, Anchovy, Herring, Flounder, Sole,

=== What do players think about trick shots?
  0.441 👍 i never fake a trick shot
  0.474 👍 I'll throw in some extra cash for the catch if you use a trick shot, the trickier the better.
  0.477 👍 The amount of trickshots I've had is with my friend's head (intentional or not).
10/10
would do 360 no scope his head.
  0.487 👍 Oh that's what I was doing wrong, I needed to rack up my trickshot multiplier, I see now
  0.496 👍 I don't think I've hit this many trickshots since 2012. 10/10

=== Is the game scary
  0.388 👍 Scary, very scary. But it's fun, and there's always something to do with friends
  0.417 👍 This game scares me(I am a fish) also my friend scares me now.
  0.446 👍 I fear no man.
but that thing...
[Sea Urchin]
it scares me.
  0.482 👍 yes very fishing fun scariest boss ive seeen ever
  0.485 👍 The most compelling horror game in the modern era. Truly frightening

=== Is it worth the price?($7.99 base)
  0.546 👍 cool game! worth the $4 it cost originally. since it was purchased for me, I can't complain about the price, but the pri
  0.599 👍 worth the 8 bucks if i needed to i'd pay 20 lol
  0.600 👍 I'd say its worth exactly 8 dollars if you're into this type of game.
  0.602 👍 good game, 100% worth $7.99
  0.604 👍 Bought with game along with PEAK for £7.12 , and it was worth every penny.





=== What do players think about shotgun and rifle
  only cover shotgun


=== What do players think about rifle:
  unclear answer only cover sniper rifle but not the rifle question ask the miltary rifle


=== "How do players comments about goldfish",
  bad retrieve based on question get fish or gold skin not gold fish

"What do players think about cod or tuna": matched review is not delivery meaningful analystis content

"Do players say the game is too short or runs out of content": good answer retrive

"Are there multiplayer connection or lag issues?": good answer retrive
"Should I buy it with PEAK?",  # this game sell with boundle: alsom retrive the slang of peak for the answer need rerank 
"Does it support VR?", # content relevant only 2
"Is the game supporting Mac version?", # get the content relevant but only 2
"How is the story and voice acting?", # get the content relevant over story but no voice acting: the game indeeed not has the voice acting 