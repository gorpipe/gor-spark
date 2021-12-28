FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:1.9.7

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]