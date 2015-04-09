# SEDAR

Scraping bits of SEDAR, the Canadian stock exchange filing system. This is captcha-protected so there's some OCR going on in here. 


# Reconstructing original URLs

We weren't very careful about linking our S3 addresses to the original documents

to reconstruct the original urls:

```
cat list_of_urls.txt | python reconstruct_urls.py
```