import os
import zipfile
import requests
import shutil
import tempfile

def install_canvasblocker():
    """CanvasBlocker 확장 프로그램 자동 설치"""
    extension_dir = "./extensions/canvasblocker"
    
    print("=" * 60)
    print("CanvasBlocker 설치 시작")
    print("=" * 60)
    
    # 디렉토리 생성
    if not os.path.exists("./extensions"):
        os.makedirs("./extensions")
    
    # 이미 설치되었는지 확인
    if os.path.exists(extension_dir):
        print(f"✅ CanvasBlocker 이미 설치됨: {extension_dir}")
        print(f"   디렉토리 내용: {os.listdir(extension_dir)[:5]}...")
        return True
    
    print("📥 CanvasBlocker 다운로드 중...")
    
    try:
        # 방법 1: Firefox Addon Store에서 직접 다운로드
        canvasblocker_id = "canvasblocker@kkapsner.de"
        url = f"https://addons.mozilla.org/firefox/downloads/latest/canvasblocker/latest.xpi"
        
        print(f"   다운로드 URL: {url}")
        
        # 다운로드
        response = requests.get(url, stream=True, timeout=30)
        
        if response.status_code == 200:
            # 임시 파일로 저장
            temp_dir = tempfile.mkdtemp()
            temp_file = os.path.join(temp_dir, "canvasblocker.xpi")
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"   ✅ 다운로드 완료: {os.path.getsize(temp_file)} bytes")
            
            # XPI 파일은 ZIP 형식이므로 압축 해제
            print(f"   📦 압축 해제 중...")
            with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                zip_ref.extractall(extension_dir)
            
            # 임시 파일 정리
            shutil.rmtree(temp_dir)
            
            # 설치 확인
            if os.path.exists(extension_dir):
                files = os.listdir(extension_dir)
                print(f"   ✅ 설치 완료: {extension_dir}")
                print(f"   📁 파일 목록 ({len(files)}개):")
                for file in files[:10]:  # 처음 10개 파일만 표시
                    print(f"     - {file}")
                if len(files) > 10:
                    print(f"     ... 외 {len(files)-10}개 파일")
                
                # manifest.json 확인
                manifest_path = os.path.join(extension_dir, "manifest.json")
                if os.path.exists(manifest_path):
                    print(f"   📄 manifest.json 확인됨")
                
                return True
            else:
                print(f"   ❌ 압축 해제 실패")
                return False
        else:
            print(f"   ❌ 다운로드 실패: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"   ❌ 설치 실패: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # 방법 2: GitHub에서 다운로드 시도
    print("\n🔄 GitHub에서 시도 중...")
    try:
        github_url = "https://github.com/kkapsner/CanvasBlocker/releases/latest/download/canvasblocker.xpi"
        print(f"   GitHub URL: {github_url}")
        
        response = requests.get(github_url, stream=True, timeout=30)
        
        if response.status_code == 200:
            temp_dir = tempfile.mkdtemp()
            temp_file = os.path.join(temp_dir, "canvasblocker_github.xpi")
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"   ✅ GitHub 다운로드 완료")
            
            with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                zip_ref.extractall(extension_dir)
            
            shutil.rmtree(temp_dir)
            
            if os.path.exists(extension_dir):
                print(f"   ✅ GitHub 설치 완료")
                return True
    except Exception as e:
        print(f"   ❌ GitHub 설치 실패: {e}")
    
    # 수동 설치 안내
    print("\n" + "=" * 60)
    print("📋 수동 설치 방법:")
    print("=" * 60)
    print("1. 다음 링크 접속: https://addons.mozilla.org/ko/firefox/addon/canvasblocker/")
    print("2. '추가하기' 버튼 클릭하여 Firefox에 설치")
    print("3. Firefox 주소창에 about:support 입력")
    print("4. '프로필 폴더' 행의 '폴더 열기' 클릭")
    print("5. extensions 폴더로 이동")
    print("6. 'canvasblocker@kkapsner.de.xpi' 파일을 ./extensions/canvasblocker/ 폴더에 복사")
    print("7. 압축 해제:")
    print("   - Windows: 확장자 .xpi를 .zip으로 변경 후 압축 해제")
    print("   - Mac/Linux: unzip canvasblocker@kkapsner.de.xpi -d extensions/canvasblocker/")
    print("=" * 60)
    
    return False

if __name__ == "__main__":
    install_canvasblocker()