==JRW notes on enronemail.txt data file==

==================== General info ====================

These data include email messages from 6 enron employees
(in addition to various spam messages from a variety of sources)
that were made publicly available after the company's collapse.
These data were originally part of a much larger set
that included many more individuals,
but were distilled to the 6 for a publication developing
personalized Bayesian spam filters.
Please follow the links below for
precise information regarding this data and research.

Source data: http://www.aueb.gr/users/ion/data/enron-spam/
Source publication: http://www.aueb.gr/users/ion/docs/ceas2006_paper.pdf

==================== Processing ====================

For their work, Metsis et al. (the authors)
appeared to have pre-processed the data,
not only collapsing all text to lower-case,
but additionally separating "words" by spaces,
where "words" unfortunately include punctuation.
As a concrete example, the sentence:

"Hey Jon, I hope you don't get lost out there this weekend!"

would have been reduced by Metsis et al. to the form:

"hey jon , i hope you don ' t get lost out there this weekend ! "

Upon seeing this we have reverted the data back toward its original state,
removing spaces so that our sample sentence would now look like:

"hey jon, i hope you don't get lost out there this weekend!"

so that we have at least preserved contractions and other higher-order lexical forms.
However, one must be aware that this reversion is not complete,
and that some object (specifically web sites) will be ill-formatted,
and that all text is still lower-cased.

==================== Format ====================

All messages are collated to a tab-delimited format:

ID \t SPAM \t SUBJECT \t CONTENT \n

where:

ID = string; unique message identifier
SPAM = binary; with 1 indicating a spam message
SUBJECT = string; title of the message
CONTENT = string; content of the message

Note that either of SUBJECT or CONTENT may be "NA",
and that all tab (\t) and newline (\n) characters
have been removed from both of the SUBJECT and CONTENT columns.

