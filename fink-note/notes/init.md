## 通过 archetype 方式安装 flink 的模板

$ mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.1.5
      
或者 

$ curl https://flink.apache.org/q/quickstart.sh | bash

## Mac下Homebrew更新国内源brew update卡死(完美解决，网上都不完整)

### 先更新下brew

有时brew版本太旧也会有问题

```shell
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

### 再更新国内源
```shell
#更新Homebrew
cd "$(brew --repo)"
git remote set-url origin https://mirrors.ustc.edu.cn/brew.git

#更新Homebrew-core
cd "$(brew --repo)/Library/Taps/homebrew/homebrew-core"
git remote set-url origin https://mirrors.ustc.edu.cn/homebrew-core.git

#更新Homebrew-cask（最重要的一步，很多更新完国内源依然卡就是没更新这个）
cd "$(brew --repo)"/Library/Taps/homebrew/homebrew-cask
git remote set-url origin https://mirrors.ustc.edu.cn/homebrew-cask.git
```

### 更新HOMEBREW_BOTTLE_DOMAIN
最重要
* 使用 zsh 的用户 （mac 上使用 `echo $0` 来判断你是哪种 shell）
```shell
  echo 'export HOMEBREW_BOTTLE_DOMAIN=https://mirrors.ustc.edu.cn/homebrew-bottles/' >> ~/.zshrc
  source ~/.zshrc
```

* 使用bash的用户

```shell
  echo 'export HOMEBREW_BOTTLE_DOMAIN=https://mirrors.ustc.edu.cn/homebrew-bottles/' >> ~/.bash_profile
source ~/.bash_profile
```

### 更新库

```shell
brew update -v
#或都使用下面的更新
brew update-reset 
```




