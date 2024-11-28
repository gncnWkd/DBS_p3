#include "bpt.h"

H_P * hp;

page * rt = NULL; //root is declared as global

int fd = -1; //fd is declared as global


H_P * load_header(off_t off) {
    H_P * newhp = (H_P*)calloc(1, sizeof(H_P));
    if (sizeof(H_P) > pread(fd, newhp, sizeof(H_P), 0)) {

        return NULL;
    }
    return newhp;
}


page * load_page(off_t off) {
    page* load = (page*)calloc(1, sizeof(page));
    //if (off % sizeof(page) != 0) printf("load fail : page offset error\n");
    if (sizeof(page) > pread(fd, load, sizeof(page), off)) {

        return NULL;
    }
    return load;
}

int open_table(char * pathname) {
    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC  , 0775);
    hp = (H_P *)calloc(1, sizeof(H_P));
    if (fd > 0) {
        //printf("New File created\n");
        hp->fpo = 0;
        hp->num_of_pages = 1;
        hp->rpo = 0;
        pwrite(fd, hp, sizeof(H_P), 0);
        free(hp);
        hp = load_header(0);
        return 0;
    }
    fd = open(pathname, O_RDWR|O_SYNC);
    if (fd > 0) {
        //printf("Read Existed File\n");
        if (sizeof(H_P) > pread(fd, hp, sizeof(H_P), 0)) {
            return -1;
        }
        off_t r_o = hp->rpo;
        rt = load_page(r_o);
        return 0;
    }
    else return -1;
}

void reset(off_t off) {
    page * reset;
    reset = (page*)calloc(1, sizeof(page));
    reset->parent_page_offset = 0;
    reset->is_leaf = 0;
    reset->num_of_keys = 0;
    reset->next_offset = 0;
    pwrite(fd, reset, sizeof(page), off);
    free(reset);
    return;
}

void freetouse(off_t fpo) {
    page * reset;
    reset = load_page(fpo);
    reset->parent_page_offset = 0;
    reset->is_leaf = 0;
    reset->num_of_keys = 0;
    reset->next_offset = 0;
    pwrite(fd, reset, sizeof(page), fpo);
    free(reset);
    return;
}

void usetofree(off_t wbf) {
    page * utf = load_page(wbf);
    utf->parent_page_offset = hp->fpo;
    utf->is_leaf = 0;
    utf->num_of_keys = 0;
    utf->next_offset = 0;
    pwrite(fd, utf, sizeof(page), wbf);
    free(utf);
    hp->fpo = wbf;
    pwrite(fd, hp, sizeof(hp), 0);
    free(hp);
    hp = load_header(0);
    return;
}

off_t new_page() {
    off_t newp;
    page * np;
    off_t prev;
    if (hp->fpo != 0) {
        newp = hp->fpo;
        np = load_page(newp);
        hp->fpo = np->parent_page_offset;
        pwrite(fd, hp, sizeof(hp), 0);
        free(hp);
        hp = load_header(0);
        free(np);
        freetouse(newp);
        return newp;
    }
    //change previous offset to 0 is needed
    newp = lseek(fd, 0, SEEK_END);
    //if (newp % sizeof(page) != 0) printf("new page made error : file size error\n");
    reset(newp);
    hp->num_of_pages++;
    pwrite(fd, hp, sizeof(H_P), 0);
    free(hp);
    hp = load_header(0);
    return newp;
}



int cut(int length) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}



void start_new_file(record rec) {

    page * root;
    off_t ro;
    ro = new_page();
    rt = load_page(ro);
    hp->rpo = ro;
    pwrite(fd, hp, sizeof(H_P), 0);
    free(hp);
    hp = load_header(0);
    rt->num_of_keys = 1;
    rt->is_leaf = 1;
    rt->records[0] = rec;
    pwrite(fd, rt, sizeof(page), hp->rpo);
    free(rt);
    rt = load_page(hp->rpo);
    //printf("new file is made\n");
}


char * db_find(int64_t key) {
    page * search = rt;  // 루트 페이지부터 서치 시작
    if(!search) {
        return NULL;    // 루트가 없을 경우 NULL 반환
    }
    
    while(!search->is_leaf) {  // 인터널 페이지 탐색하며 내려가기
        int i = 0;
        while(i<search->num_of_keys && key >= search->b_f[i].key) { 
            i++;    // 인터널 페이지 안에서 비교 대상이 key보다 작을때까지 이동
        }
        search = load_page(search->b_f[i].p_offset);     // 적절한 위치에서 아래로 이동 (포인터가 가리키는 곳으로)
    }
    for(int i=0; i<search->num_of_keys; i++) {  // 리프 페이지 안에서 탐색
        if(search->records[i].key == key) {   // 리프 페이지의 레코드의 키와 검색한 키가 같다면 해당 레코드의 value를 저장 후 리턴
            char * value = strdup(search->records[i].value);
            return value;
        }
    }
    return NULL;    // 검색 결과가 없는 경우 NULL 반환
}


int key_rotation_insert(page * current, off_t current_offset, page * sibling, off_t sibling_offset, int64_t * parent_key) { // rotation-insert 구현
    if (!sibling || sibling->num_of_keys >= LEAF_MAX) {
        // Sibling이 없거나 가득 찼다면 Rotation 불가
        return -1;
    }

    // 현재 노드에서 가장 큰 키를 오른쪽 Sibling으로 이동
    sibling->num_of_keys++;
    for (int i = sibling->num_of_keys - 1; i > 0; i--) {
        sibling->records[i] = sibling->records[i - 1];
    }
    sibling->records[0] = current->records[current->num_of_keys - 1];
    current->num_of_keys--;

    // 부모 키 업데이트
    *parent_key = sibling->records[0].key;

    // 갱신된 current와 sibling 저장
    pwrite(fd, current, sizeof(page), current_offset);
    pwrite(fd, sibling, sizeof(page), sibling_offset);

    return 0; // key-rotation 성공
}

off_t split(page *current, off_t current_offset, int64_t *promoted_key) {   // rotation-insert가 불가능할 경우 split
    // 새 페이지 생성   
    off_t new_offset = new_page();
    page *new_page = load_page(new_offset);

    // split 처리
    int split_point = cut(current->num_of_keys);
    if (current->is_leaf) {
        // 리프 페이지 split
        new_page->is_leaf = 1;
        new_page->num_of_keys = current->num_of_keys - split_point;
        new_page->next_offset = current->next_offset;

        // split된 키 복사
        for (int i = 0; i < new_page->num_of_keys; i++) {
            new_page->records[i] = current->records[split_point + i];
        }

        current->num_of_keys = split_point;
        current->next_offset = new_offset;

        *promoted_key = new_page->records[0].key; // 새로운 페이지의 첫 번째 키를 부모로 올림
    } else {
        // 인터널 페이지 split
        new_page->is_leaf = 0;
        new_page->num_of_keys = current->num_of_keys - split_point - 1;

        // split된 키 복사
        for (int i = 0; i < new_page->num_of_keys; i++) {
            new_page->b_f[i] = current->b_f[split_point + 1 + i];
        }

        current->num_of_keys = split_point;
        *promoted_key = current->b_f[split_point].key; // 중앙 키를 부모로 올림
    }

    // 갱신된 current와 new_page 저장
    pwrite(fd, current, sizeof(page), current_offset);
    pwrite(fd, new_page, sizeof(page), new_offset);
    free(new_page);

    return new_offset; // 새로 생성된 페이지 위치 반환
}


void update_parent_key(page *parent, off_t parent_offset, int64_t old_key, int64_t new_key) {   // 부모 페이지를 업데이트 (key-rotation이 적용되었을 때 부모 페이지가 갱신되어야함)
    for (int i = 0; i < parent->num_of_keys; i++) {
        if (parent->b_f[i].key == old_key) {
            parent->b_f[i].key = new_key;
            break;
        }
    }

    // 부모 페이지를 디스크에 기록
    pwrite(fd, parent, sizeof(page), parent_offset);
}


void insert_into_parent(page *parent, off_t parent_offset, int64_t promoted_key, off_t left_offset, off_t right_offset) {
    if (!parent) {
        // 부모가 없는 경우 새로운 루트 생성
        off_t new_root_offset = new_page();
        page *new_root = load_page(new_root_offset);

        new_root->is_leaf = 0;
        new_root->num_of_keys = 1;
        new_root->b_f[0].key = promoted_key;
        new_root->b_f[0].p_offset = left_offset;
        new_root->b_f[1].p_offset = right_offset;

        // 헤더 페이지에 새로운 루트를 설정
        hp->rpo = new_root_offset;
        pwrite(fd, hp, sizeof(H_P), 0);
        free(hp);
        hp = load_header(0);

        // 새로운 루트를 저장
        pwrite(fd, new_root, sizeof(page), new_root_offset);
        free(new_root);
        return;
    }

    // 부모 페이지에 promoted_key와 right_offset 삽입
    int i = parent->num_of_keys - 1;
    while (i >= 0 && parent->b_f[i].key > promoted_key) {
        parent->b_f[i + 1] = parent->b_f[i];
        i--;
    }
    parent->b_f[i + 1].key = promoted_key;
    parent->b_f[i + 1].p_offset = right_offset;
    parent->num_of_keys++;

    // 부모 페이지가 가득 찬 경우 split 수행
    if (parent->num_of_keys > INTERNAL_MAX) {
        int64_t new_promoted_key;
        off_t new_internal_offset = split(parent, parent_offset, &new_promoted_key);

        // 부모의 부모에 새 키 삽입
        insert_into_parent(load_page(parent->parent_page_offset), parent->parent_page_offset, new_promoted_key, parent_offset, new_internal_offset);
    }

    // 부모 페이지를 디스크에 저장
    pwrite(fd, parent, sizeof(page), parent_offset);
}


int db_insert(int64_t key, char * value) {    // 전체 insert 기능
    if(db_find(key)) {
        return -1;  // duplicate key가 insert되면 안 됨
    }
    page * leaf = rt;           // insert할 페이지 검색 시작
    off_t leaf_offset = hp->rpo;
    
    if(!leaf) {           // 루트가 없을 경우 새로운 루트를 만들어서 데이터를 insert
        record new_record = {key, ""};
        strncpy(new_record.value, value, sizeof(new_record.value)-1);
        start_new_file(new_record);
        return 0;
    }

    while(!leaf->is_leaf) {     // find와 마찬가지로 인터널 페이지를 탐색하며 리프까지 내려감
        int i = 0;
        while(i<leaf->num_of_keys && key >= leaf->b_f[i].key) {
            i++;
        }
        leaf_offset = leaf->b_f[i].p_offset;
        leaf = load_page(leaf->b_f[i].p_offset);
    }

    if(leaf->num_of_keys < 31) {    // 내려간 리프 페이지에 자리가 있을 경우 insert 시도
        int i = leaf->num_of_keys - 1;
        while (i >= 0 && leaf->records[i].key > key) {
            leaf->records[i + 1] = leaf->records[i];        // 삽입할 위치 뒤의 요소들을 한칸씩 뒤로 밀기
            i--;
        }
        leaf->records[i + 1].key = key;
        strncpy(leaf->records[i + 1].value, value, sizeof(leaf->records[i + 1].value) - 1);
        leaf->num_of_keys++;
        pwrite(fd, leaf, sizeof(page), leaf_offset);
        return 0;
    }

    
    page *right_sibling = load_page(leaf->next_offset);     // key-rotation 시도
    if (right_sibling) {
        int64_t old_key = right_sibling->b_f[0].key;    // 기존 sibling의 첫 번째 키
        int64_t parent_key;
        if (key_rotation_insert(leaf, leaf_offset, right_sibling, leaf->next_offset, &parent_key) == 0) {
            update_parent_key(load_page(leaf->parent_page_offset), leaf->parent_page_offset, old_key, parent_key);  // 부모 노드 갱신
            return 0;
        }
    }

    // key-rotation 실패 -> split 수행
    int64_t promoted_key;
    off_t new_leaf_offset = split(leaf, leaf_offset, &promoted_key);
    insert_into_parent(load_page(leaf->parent_page_offset), leaf->parent_page_offset, promoted_key, leaf_offset, new_leaf_offset);   // 부모 노드에 promoted_key 삽입

    return 0;
}


int db_delete(int64_t key) {
    if(!rt) {
        return -1;   // tree가 아예 없는 경우 삭제 불가 (non-zero value)
    }

    off_t leaf_offset = hp->rpo;
    page * leaf = rt;

    while(!leaf->is_leaf) {     // find와 마찬가지로 제거할 요소 찾아서 인터널 페이지 탐색하며 리프까지 내려가기
        int i = 0;
        while(i < leaf->num_of_keys && key >= leaf->b_f[i].key) i++;
        leaf_offset = leaf->b_f[i].p_offset;
        free(leaf);
        leaf = load_page(leaf_offset);
    }

    int i;
    for(i=0; i<leaf->num_of_keys; i++) {
        if(leaf->records[i].key == key) {       // 제거할 key를 찾으면 해당 위치의 index인 i를 저장한 채로 break
            break;
        }
    }
    if(i>=leaf->num_of_keys) {
        return -1;      // i가 해당 리프 페이지의 key 개수와 같아지면 key가 존재하지 않음
    }

    for(; i<leaf->num_of_keys-1; i++) {
        leaf->records[i] = leaf->records[i+1];      // 제거할 요소 뒤의 요소들을 한 칸씩 당겨 제거
    }
    leaf->num_of_keys--;

    pwrite(fd, leaf, sizeof(page), leaf_offset);

    return 0;
}//fin








