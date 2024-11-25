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
    
    while(!search->is_leaf) {   // 인터널 페이지 탐색하며 내려가기
        int i = 0;
        while(i<search->num_of_keys && key >= search->b_f[i].key) { 
            i++;    // 인터널 페이지 안에서 비교 대상이 key보다 작을때까지 이동
        }
        search = load_page(search->b_f[i].p_offset);     // 적절한 위치에서 아래로 이동 (포인터가 가리키는 곳으로)
    }

    for(int i=0; i<search->num_of_keys; i++) {  // 리프 페이지 안에서 탐색
        if(search->records[i].key == key) {   // 리프 페이지의 레코드의 키와 검색한 키가 같다면 해당 레코드의 value를 저장 후 리턴
            char * value = strdup(search->records[i].value);
            free(search);
            return value;
        }
    }

    free(search);
    return NULL;    // 검색 결과가 없는 경우 NULL 반환
}

int db_insert(int64_t key, char * value) {
    if(db_find(key)) {
        return -1;  // duplicate key가 insert되면 안 됨
    }

    page * leaf = rt;
    off_t leaf_offset = 0;

    if(!leaf) {
        record new_record = {key, ""};
        strncpy(new_record.value, value, sizeof(new_record.value)-1);
        start_new_file(new_record);
        return 0;
    }

    while(!leaf->is_leaf) {
        int i = 0;
        while(i<leaf->num_of_keys && key >= leaf->b_f[i].key) {
            i++;
        }
        leaf_offset = leaf->b_f[i].p_offset;
        leaf = load_page(leaf->b_f[i].p_offset);
    }

    if(leaf->num_of_keys < 31) {
        int i = leaf->num_of_keys - 1;
        while (i >= 0 && leaf->records[i].key > key) {
            leaf->records[i + 1] = leaf->records[i];
            i--;
        }
        leaf->records[i + 1].key = key;
        strncpy(leaf->records[i + 1].value, value, sizeof(leaf->records[i + 1].value) - 1);
        leaf->num_of_keys++;
        pwrite(fd, leaf, sizeof(page), leaf_offset);
        free(leaf);
        return 0;
    }

}


int db_delete(int64_t key) {

}//fin








