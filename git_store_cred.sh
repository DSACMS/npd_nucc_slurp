#!/bin/bash
git config credential.helper store
# remember for two months
git config credential.helper 'cache --timeout=5259600'
