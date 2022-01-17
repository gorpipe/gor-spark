FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:1.9.8

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]