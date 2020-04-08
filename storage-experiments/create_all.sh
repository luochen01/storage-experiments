source ./base.sh

echo 'create_antimatter_index'
./create_antimatter_index.sh 1
./finalize_antimatter_index.sh 1

echo 'create_antimatter'
./create_antimatter.sh 1
./finalize_antimatter.sh 1

echo 'create_deletebtree_index'
./create_deletebtree_index.sh 1
./finalize_deletebtree_index.sh 1

echo 'create_inplace'
./create_inplace.sh 1
./finalize_inplace.sh 1

echo 'create_insert_nopk'
./create_insert_nopk.sh 1
./finalize_insert_nopk.sh 1

echo 'create_insert'
./create_insert.sh 1
./finalize_insert.sh 1


echo 'create_validation_index_nocorrelate'
./create_validation_index_nocorrelate.sh 1
./finalize_validation_index_nocorrelate.sh 1

echo 'create_validation_index'
./create_validation_index.sh 1
./finalize_validation_index.sh 1


echo 'create_validation_nocorrelate'
./create_validation_nocorrelate.sh 1
./finalize_validation_nocorrelate.sh 1

echo 'create_validation_norepair_index'
./create_validation_norepair_index.sh 1
./finalize_validation_norepair_index.sh 1

echo 'create_validation_norepair'
./create_validation_norepair.sh 1
./finalize_validation_norepair.sh 1

echo 'create_validation'
./create_validation.sh 1
./finalize_validation.sh 1
