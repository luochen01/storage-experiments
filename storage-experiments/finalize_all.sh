source ./base.sh

echo 'create_antimatter_index'
./finalize_antimatter_index.sh 1 drop

echo 'create_antimatter'
./finalize_antimatter.sh 1 drop

echo 'create_deletebtree_index'
./finalize_deletebtree_index.sh 1 drop

echo 'create_inplace'
./finalize_inplace.sh 1 drop

echo 'create_insert_nopk'
./finalize_insert_nopk.sh 1 drop

echo 'create_insert'
./finalize_insert.sh 1 drop


echo 'create_validation_index_nocorrelate'
./finalize_validation_index_nocorrelate.sh 1 drop

echo 'create_validation_index'
./finalize_validation_index.sh 1 drop


echo 'create_validation_nocorrelate'
./finalize_validation_nocorrelate.sh 1 drop

echo 'create_validation_norepair_index'
./finalize_validation_norepair_index.sh 1 drop

echo 'create_validation_norepair'
./finalize_validation_norepair.sh 1 drop

echo 'create_validation'
./finalize_validation.sh 1 drop
